#! /bin/python

import psycopg2
import sys
conector = psycopg2.connect(database="workgroup", user = "", password = "", host = "", port = "8060")

cur = conector.cursor()

datasource=sys.argv[1]

query="""SELECT "workbooks"."repository_url" AS "url",
  "_background_tasks"."id" AS "id",
  "_background_tasks"."created_at" AS "created_at",
  "_background_tasks"."completed_at" AS "completed_at",
  "_background_tasks"."finish_code" AS "finish_code",
  "_background_tasks"."job_type" AS "job_type",
  "_background_tasks"."progress" AS "progress",
  "_background_tasks"."started_at" AS "started_at",
  "_background_tasks"."job_name" AS "job_name",
  "_background_tasks"."priority" AS "priority",
  "_background_tasks"."title" AS "title"
FROM "public"."_background_tasks" "_background_tasks"
  LEFT JOIN "public"."workbooks" ON ("_background_tasks"."title" = "workbooks"."name")
WHERE "_background_tasks"."job_name" LIKE 'Refresh%' AND "_background_tasks"."subtitle" LIKE 'Datasource' AND "_background_tasks"."title" LIKE '""" + datasource + """'
ORDER BY "_background_tasks"."started_at" DESC LIMIT 1"""


cur.execute(query)
rows = cur.fetchall()

for x in rows:
	#print("Created at " + str(x[2]) )
	#print("Completed at " + str(x[3]))
	#print("Complete code :" + str(x[4]))
	#print("Progress :" +  str(x[6]))
	#print(x[:])
	if x[4] == 1 and str(x[3]) == 'None':
		print("Running")
	elif x[4] == 1 and str(x[3]) != 'None':
		print("Failed/Skipped")
	elif x[4] == 0 and str(x[3]) != 'None':
		print("Completed")
conector.close()
