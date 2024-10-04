import csv

correctedArray={}
counter=0



data_initial = open("all_logins.tsv", "rU")
data = csv.DictReader((line.replace('\0','') for line in data_initial), delimiter="\t")
for row in data:
	counter+=1
	if counter%100000==0:
        	print(counter)
"""
with open('all_logins.tsv') as AllLogins:
#with open('all_logins_test_file.tsv') as AllLogins:
	loginsIterable=csv.DictReader(AllLogins,"utf-16",delimiter='\t')
	for row in loginsIterable:
		counter+=1
		if counter%100000==0:
			print(counter)
		if row['account_number'] in correctedArray:
			if row['myspectrum']<> 'NULL' and (row['myspectrum'] > correctedArray[row['account_number']]['myspectrum'] or correctedArray[row['account_number']]['myspectrum']=='NULL'):
				correctedArray[row['account_number']]['myspectrum']=row['myspectrum']
			if row['spectrumbusiness']<> 'NULL' and (row['spectrumbusiness'] > correctedArray[row['account_number']]['spectrumbusiness'] or correctedArray[row['account_number']]['spectrumbusiness']=='NULL'):
				correctedArray[row['account_number']]['spectrumbusiness']=row['spectrumbusiness']
			if row['specnet']<>'NULL' and (row['specnet'] > correctedArray[row['account_number']]['specnet'] or correctedArray[row['account_number']]['specnet']=='NULL'):
				correctedArray[row['account_number']]['specnet']=row['specnet']
			if row['division_id']<>'NULL':
				if correctedArray[row['account_number']]['division_id']<>row['division_id'] and correctedArray[row['account_number']]['division_id'] <> 'NULL':
					print(row['account_number'], " has both ", correctedArray[row['account_number']]['division_id']," and ",row['division_id']," as division_id")
				correctedArray[row['account_number']]['division_id']=row['division_id']
			if row['site_sys']<>'NULL':
	        		if correctedArray[row['account_number']]['site_sys']<>row['site_sys'] and correctedArray[row['account_number']]['site_sys']<> 'NULL':
                			print(row['account_number'], " has both ", correctedArray[row['account_number']]['site_sys']," and ",row['site_sys'], "as site_sys")
			        correctedArray[row['account_number']]['site_sys']=row['site_sys']
		        if row['prn']<>'NULL':
                		if correctedArray[row['account_number']]['prn']<>row['prn'] and correctedArray[row['account_number']]['prn']<>'NULL':
                        		print(row['account_number'], " has both ", correctedArray[row['account_number']]['prn']," and ",row['prn']," as prn")
		                correctedArray[row['account_number']]['prn']=row['prn']
		        if row['agn']<>'NULL':
                		if correctedArray[row['account_number']]['agn']<>row['agn'] and correctedArray[row['account_number']]['agn']<>'NULL':
                        		print(row['account_number'], " has both ", correctedArray[row['account_number']]['agn']," and ",row['agn']," as agn")
                		correctedArray[row['account_number']]['agn']=row['agn']		
		else:
			correctedArray[row['account_number']]=row

with open('corrected_file.tsv','w') as writefile:
	fieldnames=['account_number','myspectrum','specnet','spectrumbusiness','date_denver','division_id','site_sys','prn','agn']
	writer=csv.DictWriter(writefile,fieldnames,dialect='excel-tab')
	writer.writeheader()
	for account in correctedArray:
		writer.writerow(correctedArray[account])
"""


"""
#For debugging/testing	
for account in correctedArray:
	print(account,correctedArray[account]['division_id'],correctedArray[account]['site_sys'],correctedArray[account]['prn'],correctedArray[account]['agn'])
			
"""
