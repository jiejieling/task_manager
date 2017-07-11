#!/usr/bin/env python
#-*- coding:utf8 -*-

'''
@attention:  This script used for managing the cron job
@Author: Seven
@contact:  sevenling@shijiebang.net
@Ver: v1.1

@ExitCode:
	0	Normal
	1	Connect to DB error
	2	SSH to Host error
	3	Exec app error
'''

import os, sys, time, datetime
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.cursor import MySQLCursor, MySQLCursorDict
import paramiko, signal, argparse
from multiprocessing import Process
import smtplib, urllib2, urllib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
try:
	from hashlib import md5
except:
	from md5 import md5

#DB INFO
M_DB_CONFIG = {'host': '10.6.1.81',
			 'port': 3306,
			 'user': 'tasker',
			 'password': '',
			 'database': 'tasks',
	}

S_DB_CONFIG = M_DB_CONFIG

#SSH INFO
SSH_USER = 'root'
SSH_KEY = '~/.ssh/id_rsa'
SSH_APP_DIR = '/opt/scripts/tasks/'

#MAIL INFO
MAIL_HOST = '10.6.5.91'
MAIL_USER = 'noreply@shijiebang.com'
MAIL_PASS = ''
SENDER = MAIL_USER
REPORT_RECVER = ['sevenling@shijiebang.net']

#SMS API
SMS_API = ''
SMS_CALLER = 'sys-zabbix'
SMS_TNO = 1190010

#APP INTERPRETER
INTERPRETERS = {'sh': 'bash',
				'py': 'python',
				'pyc': 'python',
				'pyo': 'python',
				'pl': 'perl',
				'php': 'php'
				}

#GLOBAL VAR BEGIN#
log = ''
processes = {}
#GLOBAL VAR END#

def md5_file(fp):
	m = md5()
	while True:
		d = fp.read(10240)
		if not d:
			break
		m.update(d)

	return m.hexdigest()

def get_month_day(today):
	'''Get month days'''
	mon_has_31_day = [1, 3, 5, 7, 8, 10,  12]
	mon_has_30_day = [4, 6, 9, 11]
	
	if today.month == 2:
		return 28 if today.year % 4 else 29
	elif today.month in mon_has_30_day:
		return 30
	else:
		return 31

def send_mail(subject, content, content_type, addrs):
	'''Send Mail'''
	
	if not subject or not content or not addrs:
		return False
	
	msg = MIMEMultipart()
	txt = MIMEText(content, content_type, 'utf8')
	msg.attach(txt)
	msg['Subject'] = subject.decode('utf8')
	msg['From'] = 'noreply@shijiebang.com'
	mailto_list = addrs
	msg['To'] = ",".join(mailto_list)
	
	s = smtplib.SMTP()
	s.connect(MAIL_HOST)
	s.login(MAIL_USER, MAIL_PASS)
	s.sendmail(SENDER, mailto_list, msg.as_string())
	s.close()
	
	return True

def send_weixin(content, addrs):
	'''Send WeiXin'''
	
	if not content or not addrs:
		return False
	
	for addr in addrs:
		try:
			response = urllib2.urlopen(urllib2.Request(addr, content))
			response.close()
		except:
			return False
		
	return True

def send_sms(content, phones):
	'''Send SMS'''
	
	if not content or not phones:
		return False
	
	data = {'message': content, 'caller': SMS_CALLER, 'tno': SMS_TNO}
	
	for phone in phones:
		data['mobile'] = phone
		try:
			response = urllib2.urlopen(urllib2.Request(SMS_API, urllib.urlencode(data)))
			response.close()
		except:
			return False
		
	return True

class Log(object):
	'''Log Genernal Class'''
	def __init__(self, log_file):
		self.log_file = log_file
		self.open()
	
	def open(self):
		self.log = open(self.log_file, 'a')
		
	def write(self, content):
		self.log.write('[%s] %s\n'%(time.strftime('%Y-%m-%d %H:%M:%S'), content))
		self.log.flush()

	def close(self):
		self.log.close()

class SSH(object):
	'''SSH General Class'''
	def __init__(self, host, port = 22, user = 'root', private_key = None, password = None):
		self.errno = ''
		self.host = host
		self.port = port
		self.user = user
		self.key = private_key
		self.password = password
	
	def __enter__(self):
		self.s = paramiko.SSHClient()
		self.s.load_system_host_keys()
		self.s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		try:
			self.s.connect(self.host, self.port, self.user, self.password, key_filename = os.path.expanduser(self.key), timeout = 3)
		except Exception, e:
			self.errno = e
		
		return self
	
	def exec_cmd(self, cmds):
		chan = self.s.get_transport().open_session()
		try:
			chan.exec_command(cmds)
			exit_code = chan.recv_exit_status()
			stdout = chan.makefile('r', -1)
			stderr = chan.makefile_stderr('r', -1)
			stdout_buffer = stdout.read()
			stderr_buffer = stderr.read()
			stdout.close()
			stderr.close()
			
		except Exception, e:
			self.errno = e
			exit_code = -1
			stdout_buffer = ''
			stderr_buffer = ''
		
		finally:
			chan.close()
		
		return  exit_code, stdout_buffer, stderr_buffer
	
	def sput(self, src, dst):
		sftp = self.s.get_transport().open_sftp_client()
		try:
			sftp.put(src, dst)
		except Exception, e:
			self.errno = e
			return False
		
		finally:
			sftp.close()
			
		return True

	def sget(self, dst, src):
		sftp = self.s.get_transport().open_sftp_client()
		try:
			sftp.get(dst, src)
		except Exception, e:
			self.errno = e
			return False
		
		finally:
			sftp.close()
			
		return True
				
	
	def smd5_file(self, filepath):
		sftp = self.s.get_transport().open_sftp_client()
		
		try:
			fp = sftp.open(filepath)
			
		except:
			return True, ''
		
		md5sum = md5_file(fp)
		
		fp.close()
		sftp.close()
		
		return True, md5sum
		
	def __exit__(self,exc_type, exc_value, traceback):
		try:
			self.s.close()
		except Exception ,e:
			self.errno = e
		

class MyDB(object):
	'''MySQL Operation Class'''
	def __init__ (self, master_config, slave_config):
		self.db_config = {'connection_timeout':3,
						  'autocommit': True,
						  'failover': [master_config, slave_config]
						}

	def connect(self): 
		for i in range(3):
			try:
				self.conn = mysql.connector.connect(**self.db_config)
				return True, None
			except mysql.connector.Error, err:
				if err.errno in (errorcode.CR_SERVER_LOST, errorcode.CR_SERVER_LOST_EXTENDED):
					continue
				
				return False, err
		
		return False, err

	def modify(self, sql, cursor_type = MySQLCursor):
		try:
			cur = self.conn.cursor(cursor_class = cursor_type)
			res = cur.execute(sql)
			return True, res
		except mysql.connector.Error, err:
			if err.errno in (errorcode.CR_SERVER_LOST, errorcode.CR_SERVER_LOST_EXTENDED):
				ret, info = self.connect()
				if ret:
					return self.modify(sql, cursor_type)
				else:
					return False, info
			
			return False, err
		finally:
			cur.close()

	def select(self,sql, cursor_type = MySQLCursor):
		try:
			cur = self.conn.cursor(cursor_class = cursor_type)
			cur.execute(sql)
			ret = cur.fetchall()
			return True, ret
		except mysql.connector.Error, err:
			if err.errno in (errorcode.CR_SERVER_LOST, errorcode.CR_SERVER_LOST_EXTENDED):
				ret, info = self.connect()
				if ret:
					return self.select(sql, cursor_type)
				else:
					return False, info
		
			return False, err
		finally:
			cur.close()

	def close(self):  
		self.conn.close()

class Work(Process):
	'''Get Job, And Do It'''
	def __init__(self, primary_id, host, inter, app, args = '', daemon_flag = 0, **alarm_info):
		super(Work, self).__init__()
		self.primary_id = primary_id
		self.host = host
		self.inter = inter
		self.app = app
		self.args = args
		self.daemon_flag = daemon_flag
		self.alarm_info = alarm_info
	
	def init_db(self):
		self.db = MyDB(M_DB_CONFIG, S_DB_CONFIG)
		ret = self.db.connect()
		if not ret[0]:
			return False, ret[1]
		
		return True, ''
		
	def update_status(self, status, update_time = ''):
		if update_time:
			SQL = 'UPDATE task_info SET status = %i, update_time = "%s" WHERE id = %i'%(status, update_time, self.primary_id)
		else:
			SQL = 'UPDATE task_info SET status = %i WHERE id = %i'%(status, self.primary_id)
			
		self.db.modify(SQL)	
		return True
	
	def insert_log(self, start_time, end_time, cost, code, output):
		SQL = 'INSERT INTO task_log(task_id, task_start, task_end, task_cost, task_result, task_output, update_time) \
									 VALUES(%i, "%s", "%s", %.2f, %i, "%s", "%s")' \
									      %(self.primary_id, start_time, end_time, cost, code, output, time.strftime('%Y-%m-%d %H:%M:%S'))
		self.db.modify(SQL)
		return True
	
	def alarm(self, code, output):
		alarm_content = []
		alarm_content.append('[%s] Exec %s %s on %s'%(time.strftime('%Y-%m-%d %H:%M:%S'), self.app, self.args, self.host))
		alarm_content.append('result: %i'%code)
		alarm_content.append('output: %s'%output)
		alarm_content = '\n'.join(alarm_content)
		
		alarm_type = self.alarm_info.get('alarm_type', 0x00)
		if alarm_type & 0x01 :
			send_mail('Task Manager Report', alarm_content, 'plain', self.alarm_info.get('mails', '').split(','))
		
		if alarm_type & (0x01 << 1):
			send_weixin(alarm_content, self.alarm_info.get('weixins', '').split(','))
		
		if alarm_type & (0x01 << 2):
			send_sms(alarm_content, self.alarm_info.get('phones', '').split(','))
		
		return True
	
	def run(self):
		ret, info = self.init_db()
		if not ret:
			log.write('Connect to DB err:%s'%info)
			sys.exit(1)
		
		self.update_status(1)

		start_time = time.time()
		with SSH(host = self.host, port = 22, user = SSH_USER, private_key = SSH_KEY) as ssh:
			end_time = time.time()
			if ssh.errno:
				log.write("SSH to Host %s err:%s"%(self.host, ssh.errno))
				self.update_status(3)
				self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, -1, "SSH to Host %s err:%s"%(self.host, ssh.errno))
				self.alarm(-1, "SSH to Host %s err:%s"%(self.host, ssh.errno))
				sys.exit(2)
			
			if self.app and not os.path.exists(self.app):
				log.write("App %s not found"%self.app)
				self.update_status(3)
				self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, -1, "App %s not found"%self.app)
				self.alarm(-1, "App %s not found"%self.app)
				sys.exit(3)
			
			if self.app:
				with open(self.app) as fp:
					src_md5sum = md5_file(fp)
				
				ret, dst_md5sum = ssh.smd5_file(SSH_APP_DIR + os.path.basename(self.app))
			
				if src_md5sum != dst_md5sum and not ssh.sput(self.app, SSH_APP_DIR + os.path.basename(self.app)):
					end_time = time.time()
					log.write("SCP %s to %s:%s err:%s"%(self.app, self.host, SSH_APP_DIR, ssh.errno))
					self.update_status(3)
					self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, -1, "SCP %s to %s:%s err:%s"%(self.app, self.host, SSH_APP_DIR, ssh.errno))
					self.alarm(-1, "SCP %s to %s:%s err:%s"%(self.app, self.host, SSH_APP_DIR, ssh.errno))
					sys.exit(3)
				
				prefix = self.inter if self.inter else INTERPRETERS.get(os.path.basename(self.app).split('.')[-1], '')
				code, stdout, stderr = ssh.exec_cmd("%s %s %s"%(prefix, SSH_APP_DIR + os.path.basename(self.app), self.args))
			elif self.inter:
				prefix = self.inter
				code, stdout, stderr = ssh.exec_cmd("%s %s"%(prefix, self.args))
			else:
				log.write("Cmd %s %s %s invalid"%(self.inter, self.app, self.args))
				self.update_status(3)
				self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, -1, "Cmd %s %s %s invalid"%(self.inter, self.app, self.args))
				self.alarm(-1, "Cmd %s %s %s invalid"%(self.inter, self.app, self.args))
				sys.exit(3)
								
			end_time = time.time()
			if ssh.errno:
				log.write("Exec %s %s %s on %s err:%s"(prefix, self.app, self.args, self.host, ssh.errno))
				self.update_status(3)
				self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, -1, "Exec %s %s %s on %s err:%s"(prefix, self.app, self.args, self.host, ssh.errno))
				self.alarm(-1, "Exec %s %s %s on %s err:%s"(prefix, self.app, self.args, self.host, ssh.errno))
				sys.exit(3)	

			output = [i for i in [stdout, stderr] if i]
			
			self.update_status(2, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)))
			self.insert_log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)), time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)), end_time - start_time, code, '\n'.join(output))
			if code != 0:
				self.alarm(code, output)
	
def day_report():
	db = MyDB(M_DB_CONFIG, S_DB_CONFIG)
	ret = db.connect()
	if not ret[0]:
		log.write('Connect to DB err:%s'%(ret[1]))
		sys.exit(1)
	
	yestday = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")
	
	SQL = 'SELECT COUNT(*), COUNT(DISTINCT task_id) FROM task_log WHERE update_time >= "%s"'%yestday
	ret, lines = db.select(SQL)
	if not ret:
		log.write('Exec SQL:%S error:%s'(SQL, lines))
		return False
	
	log_count, task_count = lines[0]
	
	SQL = 'SELECT COUNT(*) FROM task_log WHERE task_result != 0 AND update_time >= "%s"'%yestday
	ret, lines = db.select(SQL)
	if not ret:
		log.write('Exec SQL:%S error:%s'(SQL, lines))
		return False
	
	err_count = lines[0][0]
	
	content = []
	content.append('Exec count: %i'%log_count)
	content.append('Exec task count: %i'%task_count)
	content.append('Exec error count: %i'%err_count)

	send_mail('Task Manager Report About %s'%yestday, '\n'.join(content), 'plain', REPORT_RECVER)
	
	return True


def chld_sig_handle(signum, frame):
	'''child signal handle func'''
	pid, ret = os.wait()
	del processes[pid]
	log.write("Child process %i exit..."%pid)
	
def term_sig_handle(signum, frame):
	'''Terminal signal handle func'''
	log.write("Recv %i signal, program will exit..."%signum)
	
	signal.setitimer(signal.ITIMER_REAL, 0, 0)
	
	while processes:
		time.sleep(1)
	
	log.close()
	sys.exit(0)
	
def alarm_sig_handle(signum, frame):
	'''Alarm signal handle func'''
	
	log.write('Get alarm signal')
	db = MyDB(M_DB_CONFIG, S_DB_CONFIG)
	ret = db.connect()
	if not ret[0]:
		log.write('Connect to DB err:%s'%(ret[1]))
		sys.exit(1)
	
	cur_time = time.localtime()
	cur_datetime = datetime.datetime.now()
	
	if cur_time.tm_hour == 0 and cur_time.tm_min == 0:
		day_report()
	
	#find waiting for execing task 
	SQL = 'SELECT task_info.id, minute, hour, day, month, week, host, script_inter, script_path, script_args, is_daemon, alarm_info, status, update_time, \
		   group_concat(group_weixin) as weixins, group_concat(mail) as mails, group_concat(phone) as phones \
		   FROM task_info \
		   LEFT JOIN groups \
		   ON task_info.group_id = groups.id \
		   LEFT JOIN members \
		   ON groups.id = members.group_id \
		   WHERE on_off = 1 AND status != 1 GROUP BY task_info.id'
	
	ret, lines = db.select(SQL, MySQLCursorDict)
	if ret:
		for line in lines:
			step = line['week'].split('/')[-1]
			if step in ('*', str(cur_time.tm_wday + 1)) or (step != line['week'] and line['update_time'] + datetime.timedelta(weeks = int(step)) <= cur_datetime):
				step = line['month'].split('/')[-1]
				if step in ('*', str(cur_time.tm_mon)) or (step != line['month'] and line['update_time'] + datetime.timedelta(days = int(step) * get_month_day(line['update_time'])) <= cur_datetime):
					step = line['day'].split('/')[-1]
					if step in ('*', str(cur_time.tm_mday)) or (step != line['day'] and line['update_time'] + datetime.timedelta(days = int(step)) <= cur_datetime):
						step = line['hour'].split('/')[-1]
						if step in ('*', str(cur_time.tm_hour)) or (step != line['hour'] and line['update_time'] + datetime.timedelta(hours = int(step)) <= cur_datetime):
							step = line['minute'].split('/')[-1]
							if step in ('*', str(cur_time.tm_min)) or (step != line['minute'] and line['update_time'] + datetime.timedelta(minutes = int(step)) <= cur_datetime):
								p = Work(line['id'], line['host'], line['script_inter'], line['script_path'], line['script_args'], line['is_daemon'], alarm_type = line['alarm_info'], weixins = line['weixins'], mails = line['mails'], phones = line['phones'])
								p.start()
								processes.setdefault(p.pid, True)
								
	else:
		log.write('Get task info error:%s'%lines)
	

def daemon():
	'''Enter Daemon Mode'''
	os.umask(0)
	
	pid = os.fork()        
	if pid > 0:
		sys.exit(0)
	
	os.setsid()
	pid = os.fork()
	if pid > 0:
		sys.exit(0)
	
	os.closerange(0, 1024)
	
	sys.stdin = open('/dev/null', "w+")
	os.dup(sys.stdin.fileno())
	os.dup(sys.stdin.fileno())

def main(log_file):
	'''Enter'''
	global log
	
	daemon()

	log = Log(log_file)
	paramiko.util.log_to_file(os.path.dirname(log_file) + '/paramiko.log', 'WARN')
	log.write("Program start..")
	
	signal.signal(signal.SIGCHLD, chld_sig_handle)
	signal.signal(signal.SIGTERM, term_sig_handle)
	signal.signal(signal.SIGINT, term_sig_handle)
	signal.signal(signal.SIGALRM, alarm_sig_handle)
	signal.setitimer(signal.ITIMER_REAL, 1, 60)
	
	while True:
		signal.pause()
	
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Task Manager.')
	parser.add_argument('--log',
						help = "special log file path",
						required = False,
						default = '/opt/logs/task-manager/task.log')
	
	args = parser.parse_args()

	if args.log:
		main(args.log)
