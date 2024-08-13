#  3rd and 4th and 5th termwork
```py
cd Documents/
```
```py
nano Term4data.csv
```
```py
1, M, 25000, 2, Agree
2, F, 50000, 1, Disagree
3,M,75000,0,Neutral
4,F,80000,2,Agree
5,F,10000,1,DisAgree
6,F,20000,3,Neutral
```
```py
nano mapper.py 
```
```py
cat Term4data.csv| python mapper.py 
```
```py
nano reducer.py 
```
```py
cat Term4data.csv| python mapper.py | python reducer.py 
```
```py
start-all.sh
```
```py
jps
```
```py
hdfs dfs -mkdir /T4
```
```py
hdfs dfs -copyFromLocal /home/hduser/Documents/Term4data.csv  /T4
```
```py
hdfs dfs -ls /
```
```py
chmod 777 mapper.py reducer.py 
```
```py
ls -l
```
```py
$ hadoop jar /home/hduser/Documents/hadoop-streaming-2.7.3.jar \
```
```py
-input  /T4/ Term4data.csv \
```
```py
-output  /T4/output \
```
```py
-mapper  /home/hduser/Documents/mapper.py \
```
```py
-reducer  /home/hduser/Documents/reducer.py 
```
```py
hdfs dfs -cat /T4 /output/part-00000
```
# 5 twermwork csv file

```py
E002, Harsh, IT, 50000
E003, Ragini, IT, 75000
E004, Mithun, Accounts, 20000 
E005, Pruthavi, Marketing, 45000
E006, Anjali, IT, 70000
E007, Kunal, Marketing, 60000
E008, Mitali, Accounts, 55000
```
# 3: Map Reduce program to calculate the average age for each gender.
# 3. mapper.py

```py
#!/usr/bin/python
import sys
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    if len(line) >=2:
        gender = line[1]
        age = line[2]
        print '%s\t%s' % (gender, age)
```

# 3. reducer.py

```py
#!/usr/bin/python
import sys
gender_age = {}
for line in sys.stdin:
    line = line.strip()
    gender, age = line.split('\t')
    if gender in gender_age:
        gender_age[gender].append(int(age))
    else:
        gender_age[gender] = []
        gender_age[gender].append(int(age))
for gender in gender_age.keys():
    ave_age = sum(gender_age[gender])*1.0 / len(gender_age[gender])
    print '%s\t%s'% (gender, ave_age)
```

#
# 4. TERMWORK4
# 4. mapper.py

```py
#!/usr/bin/env python
import sys
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    if len(line) >=2:
        pid = line[0]
        opinion = line[4]
        print '%s\t%s' % (pid, opinion)
```

# 4. reducer.py

```py
#!/usr/bin/env python
import sys
opiniondic={}
count=0
for line in sys.stdin:
    line = line.strip()
    pid, opinion = line.split('\t')
    if opinion in opiniondic:
        opiniondic[opinion].append(count+1)
    else:
        opiniondic[opinion] = []
        opiniondic[opinion].append(count+1)
for op in opiniondic.keys():
    count=len(opiniondic[op])
    print '%s\t%s'% (op,count)
```


# 5. TERMWORK5
# 5. mapper.py

```py
#!/usr/bin/env python
import sys
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    if len(line) >=2:
        dept = line[2]
        sal = line[3]
        print '%s\t%s' % (dept, sal)
```

# 5. reducer.py

```py
#!/usr/bin/env python
import sys
deptdic={}


for line in sys.stdin:
    line = line.strip()
    dept,sal = line.split('\t')
    if dept in deptdic:
        deptdic[dept].append(int(sal))
    else:
        deptdic[dept] = []
        deptdic[dept].append(int(sal))
for dept in deptdic.keys():
    sum_sal = sum(deptdic[dept])
    print '%s\t%s'% (dept,sum_sal)
```
