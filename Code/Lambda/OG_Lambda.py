import json
import psycopg2
import rds_config
import datetime
import logging

def fullnum(strnum):
	lastchar = strnum[-1:]
	therest = strnum[:-1]
	out=0
	if lastchar.upper() == "B":
		out = float(therest)*1
	elif lastchar.upper() == "K":
		out = float(therest)*1000
	elif lastchar.upper() == "M":
		out = float(therest)*1000000
	elif lastchar.upper() == "G":
		out = float(therest)*1000000000
	elif lastchar.upper() == "T":
		out = float(therest)*1000000000000
	else:
		out = float(strnum)
	
	return out

def lambda_handler(event, context):

	response_code = 200
	response_body = "Success"

	try: 
		# FOR PRODUCTION USE: (parse string as json)
		ebody = json.loads(event["body"])
	except:
		# FOR DEV/TEST USE: (input is already json)
		print ("Debug/Test mode")
		ebody = event["body"]
	
	try:
		connection = psycopg2.connect(user=rds_config.db_username, ## rds + postgres??
									  password=rds_config.db_password,
									  host=rds_config.db_endpoint,
									  port=rds_config.db_port,
									  database=rds_config.db_name)
		
		cursor = connection.cursor()
	   
	except (Exception, psycopg2.Error) as error :
		response_code = 500
		response_body = "Internal Database Connection Error"
		print ("ERROR: fetching data from PostgreSQL", error)
		
	
	try:
		data_cc = ebody["cc"] # ebody is event body
	
		cursor.execute("""select project_id, org_name, proj_name from projects where cc_key = %s;""", (data_cc,))
		recs = cursor.fetchall()
		for row in recs:
			proj_id = row[0]
			org_name = row[1]
			proj_name = row[2]
		cursor.close()
		hn = ebody["host"]
		ts = ebody["ts"]
		os = ebody["os"]
	
	except:
		response_code = 400
		response_body = "Invalid cc code"
		print ("ERROR: cc not found in projects table: ", data_cc)
		print ("'ts': '", str(datetime.datetime.now()) , "', 'sev': 'ERROR', 'cc': '", data_cc, "', 'msg': ")
		
	else:
		insq = "INSERT INTO public.perf VALUES "
		
		#PROC_CPU (From TOP)
		if 'proc_cpu' in ebody:
			for i in ebody["proc_cpu"] :
				try:
					numtest = float(ebody["proc_cpu"][i]["pcpu"])
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Process Utilization', ebody["proc_cpu"][i]["comm"], i, ebody["proc_cpu"][i]["pcpu"])
				except:
					response_body += " Invalid proc_cpu value"
					print ("WARN: Invalid proc_cpu value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		else:
			response_body += " Missing or invalid proc_cpu data"
			print ("WARN: Missing or invalid proc_cpu data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
				
		#PMEM
		if 'pmem' in ebody:
			for i in ebody["pmem"] :
				try:
					numtest = float(ebody["pmem"][i]["pmem"])
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Process Utilization', ebody["pmem"][i]["comm"], ebody["pmem"][i]["pid"], ebody["pmem"][i]["pmem"])
				except:
					response_body += " Invalid pmem value"
					print ("WARN: Invalid pmem value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)	
		else:
			response_body += " Missing or invalid pmem data"
			print ("WARN: Missing or invalid pmem data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		
		#DF
		if 'df' in ebody:
			for i in ebody["df"] :
				try:
					numtest = float(ebody["df"][i]["size"])
					numtest = float(ebody["df"][i]["used"])
					numtest = float(ebody["df"][i]["avail"])
					diskpct = float(ebody["df"][i]["pct"][:-1])/100
					numtest = float(diskpct)
					
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Size (KB)', ebody["df"][i]["mount"], ebody["df"][i]["fs"], ebody["df"][i]["size"])
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Used (KB)', ebody["df"][i]["mount"], ebody["df"][i]["fs"], ebody["df"][i]["used"])
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Available (KB)', ebody["df"][i]["mount"], ebody["df"][i]["fs"], ebody["df"][i]["avail"])
					insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Pct Utilization', ebody["df"][i]["mount"], ebody["df"][i]["fs"], diskpct)
				except: 
					response_body += " Invalid df value"
					print ("WARN: Invalid df value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		else:
			response_body += " Missing or invalid df data"
			print ("WARN: Missing or invalid df data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		
	
		#TOP parsing
		if 'topst' in ebody:
			try:
				numtest = float(ebody["topst"]["num_procs"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Num Procs', '', '', ebody["topst"]["num_procs"])
			except: 
				response_body += " Invalid topst-num_procs value"
				print ("WARN: Invalid topst-num_procs value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["topst"]["ld_avg1"])	
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Load Avg 1min', '', '', ebody["topst"]["ld_avg1"])
			except: 
				response_body += " Invalid topst-ld_avg1 value"
				print ("WARN: Invalid topst-ld_avg1 value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["topst"]["cpu_usr"])		
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'User Pct', '', '', ebody["topst"]["cpu_usr"])
			except: 
				response_body += " Invalid topst-cpu_usr value"
				print ("WARN: Invalid topst-cpu_usr value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
				
			try:
				numtest = float(ebody["topst"]["cpu_sys"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Sys Pct', '', '', ebody["topst"]["cpu_sys"])
			except: 
				response_body += " Invalid topst-cpu_sys value"
				print ("WARN: Invalid topst-cpu_sys value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["topst"]["cpu_idl"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Idle Pct', '', '', ebody["topst"]["cpu_idl"])
			except: 
				response_body += " Invalid topst-cpu_idl value"
				print ("WARN: Invalid topst-cpu_idl value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
				
			try:
				numtest = float(ebody["topst"]["cpu_wai"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'CPU', 'Wait Pct', '', '', ebody["topst"]["cpu_wai"])
			except: 
				response_body += " Invalid topst-cpu_wai value"
				print ("WARN: Invalid topst-cpu_wai value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		else:
			response_body += " Missing or invalid top data"
			print ("WARN: Missing or invalid top data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
		#FREE parsing
		if 'freest' in ebody:
			try:
				numtest = float(ebody["freest"]["mem_tot"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Total (KBytes)', '', '', ebody["freest"]["mem_tot"])
			except: 
				response_body += " Invalid freest-mem_tot value"
				print ("WARN: Invalid freest-mem_tot value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
				
			try:
				numtest = float(ebody["freest"]["mem_used"])	
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Used (KBytes)', '', '', ebody["freest"]["mem_used"])
			except: 
				response_body += " Invalid freest-mem_used value"
				print ("WARN: Invalid freest-mem_used value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["mem_buff"])	
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Buffer (KBytes)', '', '', ebody["freest"]["mem_buff"])
			except: 
				response_body += " Invalid freest-mem_buff value"
				print ("WARN: Invalid freest-mem_buff value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["mem_free"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Free (KBytes)', '', '', ebody["freest"]["mem_free"])
			except: 
				response_body += " Invalid freest-mem_free value"
				print ("WARN: Invalid freest-mem_free value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["swap_tot"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Swap', 'Total (KBytes)', '', '', ebody["freest"]["swap_tot"])
			except: 
				response_body += " Invalid freest-swap_tot value"
				print ("WARN: Invalid freest-swap_tot value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["swap_free"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Swap', 'Free (KBytes)', '', '', ebody["freest"]["swap_free"])
			except: 
				response_body += " Invalid freest-swap_free value"
				print ("WARN: Invalid freest-swap_free value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["swap_used"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Swap', 'Used (KBytes)', '', '', ebody["freest"]["swap_used"])
			except: 
				response_body += " Invalid freest-swap_used value"
				print ("WARN: Invalid freest-swap_used value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
			
			try:
				numtest = float(ebody["freest"]["mem_avail"])
				insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Memory', 'Available (KBytes)', '', '', ebody["freest"]["mem_avail"])
			except: 
				response_body += " Invalid freest-mem_avail value"
				print ("WARN: Invalid freest-mem_avail value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		else:
			response_body += " Missing or invalid free data "
			print ("WARN: Missing or invalid free data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
		
		#Disk Info
		if 'diskarr' in ebody:
			mounts = {}
			for i in ebody["diskarr"] :
				if (len(ebody["diskarr"][i]["mountpoint"]) != 0):
					mounts[(ebody["diskarr"][i]["maj_min"])] = [(ebody["diskarr"][i]["dev_name"]),(ebody["diskarr"][i]["type"]),(ebody["diskarr"][i]["mountpoint"])]
		else:
			response_body += " Missing or invalid diskarr data"
			print ("WARN: Missing or invalid diskarr data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
						
		#Disk Stat
		if 'diskstat0' in ebody:
			if 'diskstat1' in ebody:
				for i in ebody["diskstat0"] :
					mm = ebody["diskstat0"][i]["maj"] + ":" + ebody["diskstat0"][i]["min"]
	
					if mm in mounts :
						
						#Read Latency
						n1 = (int(ebody["diskstat1"][i]["time_spend_reading_ms"]))
						n2 = (int(ebody["diskstat0"][i]["time_spend_reading_ms"]))
						n3 = (int(ebody["diskstat1"][i]["reads_completed_successfully"]))
						n4 = (int(ebody["diskstat0"][i]["reads_completed_successfully"]))
						read_lat = ((n1 - n2) / (n3 - n4) if (n3 - n4) else 0 )
						#print (read_lat)
						try:
							numtest = float(str(read_lat))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Read Latency (ms)', mounts[mm][2], mounts[mm][0], str(read_lat))
						except: 
							response_body += " Invalid disk stat read lat value"
							print ("WARN: Invalid disk stat read lat value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
						
						#Write Latency
						n1 = (int(ebody["diskstat1"][i]["time_spend_writing_ms"]))
						n2 = (int(ebody["diskstat0"][i]["time_spend_writing_ms"]))
						n3 = (int(ebody["diskstat1"][i]["writes_completed"]))
						n4 = (int(ebody["diskstat0"][i]["writes_completed"]))
						write_lat = ((n1 - n2) / (n3 - n4) if (n3 - n4) else 0 )
						#print (write_lat)
						try:
							numtest = float(str(write_lat))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Write Latency (ms)', mounts[mm][2], mounts[mm][0], str(write_lat))
						except: 
							response_body += " Invalid disk stat write lat value"
							print ("WARN: Invalid disk stat write lat value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
							
						#Read Throughput
						n1 = (float(ebody["diskstat1"][i]["sectors_read"]))
						n2 = (float(ebody["diskstat0"][i]["sectors_read"]))
						read_thr = ((n1 - n2) / 2 / 10 / 1000 )
						#print (read_thr)
						try:
							numtest = float(str(read_thr))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Read Throughput (MBps)', mounts[mm][2], mounts[mm][0], str(read_thr))
						except: 
							response_body += " Invalid disk stat read thru value"
							print ("WARN: Invalid disk stat read thru value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
							
						#Write Throughput
						n1 = (float(ebody["diskstat1"][i]["sectors_written"]))
						n2 = (float(ebody["diskstat0"][i]["sectors_written"]))
						write_thr = ((n1 - n2) / 2 / 10 / 1000 )
						#print (write_thr)
						try:
							numtest = float(str(write_thr))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Write Throughput (MBps)', mounts[mm][2], mounts[mm][0], str(write_thr))
						except: 
							response_body += " Invalid disk stat write thru value"
							print ("WARN: Invalid disk stat write thru value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
						
						#Read IO
						n1 = (float(ebody["diskstat1"][i]["reads_completed_successfully"]))
						n2 = (float(ebody["diskstat1"][i]["reads_merged"]))
						n3 = (float(ebody["diskstat0"][i]["reads_completed_successfully"]))
						n4 = (float(ebody["diskstat0"][i]["reads_merged"]))
						read_io = ((n1 + n2 - n3 - n4)  / 10 )
						#print (read_io)
						try:
							numtest = float(str(read_io))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Read IOPS', mounts[mm][2], mounts[mm][0], str(read_io))
						except: 
							response_body += " Invalid disk stat read io value"
							print ("WARN: Invalid disk stat read io value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
						
						#Write IO
						n1 = (float(ebody["diskstat1"][i]["writes_completed"]))
						n2 = (float(ebody["diskstat1"][i]["writes_merged"]))
						n3 = (float(ebody["diskstat0"][i]["writes_completed"]))
						n4 = (float(ebody["diskstat0"][i]["writes_merged"]))
						write_io = ((n1 + n2 - n3 - n4)  / 10 )
						#print (write_io)
						try:
							numtest = float(str(write_io))	
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Write IOPS', mounts[mm][2], mounts[mm][0], str(write_io))
						except: 
							response_body += " Invalid disk stat write_io value"
							print ("WARN: Invalid disk stat write_io value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)	
						
						#Current IO
						current_io = (float(ebody["diskstat1"][i]["ios_currently_in_progress"]))
						#print (current_io)
						try:
							numtest = float(str(current_io))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'IOs in Progress', mounts[mm][2], mounts[mm][0], str(current_io))
						except: 
							response_body += " Invalid disk stat current_io value"
							print ("WARN: Invalid disk stat current_io value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
						
						#Active Time
						n1 = (float(ebody["diskstat1"][i]["time_spent_doing_ios_ms"]))
						n2 = (float(ebody["diskstat0"][i]["time_spent_doing_ios_ms"]))
						active_time = ((n1 - n2)  / 10000 )
						#print (active_time)
						try:
							numtest = float(str(active_time))
							insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Disk', 'Active Time Pct', mounts[mm][2], mounts[mm][0], str(active_time))
						except: 
							response_body += " Invalid disk stat active_time value"
							print ("WARN: Invalid disk stat active_time value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)	
		else:
			response_body += " Missing or invalid diskstat data"
			print ("WARN: Missing or invalid diskstat data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
							
		#Network Stat
		if 'netstat0' in ebody:
			if 'netstat1' in ebody:
				for i in ebody["netstat0"] :
					#Inbound KBPS
					n1 = (float(ebody["netstat1"][i]["bytes_in"]))
					n2 = (float(ebody["netstat0"][i]["bytes_in"]))
					inbound_kbps = ((n1 - n2)  / 10000 )
					#print (inbound_kbps)
					try:
						numtest = float(str(inbound_kbps))
						insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Network', 'Inbound Kbps', i, '', str(inbound_kbps))
					except: 
						response_body += " Invalid network stat inbound_kbps value"
						print ("WARN: Invalid network stat inbound_kbps value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)		
					
					#Outbound KBPS
					n1 = (float(ebody["netstat1"][i]["bytes_out"]))
					n2 = (float(ebody["netstat0"][i]["bytes_out"]))
					outbound_kbps = ((n1 - n2)  / 10000 )
					#print (outbound_kbps)
					try:
						numtest = float(str(outbound_kbps))
						insq += "(%s, '%s', '%s', '%s', '%s', '%s', '%s', %s)," % (proj_id, hn, ts, 'Network', 'Outbound Kbps', i, '', str(outbound_kbps))
					except: 
						response_body += " Invalid network stat outbound_kbps value"
						print ("WARN: Invalid network stat outbound_kbps value for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)		
		else:
			response_body += " Missing or invalid netstat data"
			print ("WARN: Missing or invalid netstat data for " + org_name + ": " + proj_name + " cc: " + data_cc + " host: " + hn)
					
					
			
		insq = insq[:-1] # end of linux
	
		try:
			cursor = connection.cursor()
			cursor.execute(insq)
			connection.commit()
			count = cursor.rowcount
			print ("Success: ", count, "record(s) inserted for ", org_name, " (", data_cc, ")")
		
		except (Exception, psycopg2.Error) as error :
			print ("ERROR: Failed to insert rows into perf table ", error)
			
		else:
			try:
				# Add to summary table
				sumq = "insert into proj_host_dates (project_id, hostname, start_ts) values (%s, '%s', '%s') on conflict on constraint proj_host_dates_pk do nothing" % (proj_id, hn, ts)
				cursor = connection.cursor()
				cursor.execute(sumq)
				connection.commit()
				
				# Update last date
				sumq = "update proj_host_dates set last_ts = '%s' where project_id = %s	and hostname = '%s'" % (ts, proj_id, hn)
				cursor = connection.cursor()
				cursor.execute(sumq)
				connection.commit()
				
			except (Exception, psycopg2.Error) as error :
				print ("ERROR: Failed to insert/update rows into summary table ", error)

		
	response = {
		'statusCode': response_code,
		'headers': {'type': 'application/json'},
		'body': response_body,
		'isBase64Encoded': 'false'
	}
	
	return response 
	
