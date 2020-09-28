from pprint import pprint
from py2neo import Graph,Node,Relationship


test_graph = Graph(
    "http://localhost:7474", 
    username="neo4j", 
    password="111111"
  )


# data1 = test_graph.run('match (a:company)<-[r1]-(b:employee)-[r2]->(c:company) return a,r1,b,r2,c')
data1 = test_graph.run('match (employee:employee)-[r]->(company:company) return employee,r,company')
data1 = list(data1)
print(data1, type(data1))
pprint(data1[0]['r'])
pprint(data1[0]['r']['employ_end'])
pprint(dict(data1[0]))
'''
{'company': (_20:company {company_name: '\u78a7\u6842\u56ed'}),
 'employee': {'employee_name': '张三',
              'id_card': '00000001'},
 'r': {'employ_end': '2020-03-30',
       'employ_start': '2020-03-05'}}
'''


employee_set = {x['employee'] for x in data1}
new_connect_list = []
for employee in employee_set:
    time_and_company_list = [(x['r']['employ_start'], x['company']) for x in data1 if x['employee'] == employee]
    time_and_company_list.sort(key=lambda x: x[0])
    # [('2020-01-01', (_0:company {company_name: '\u6052\u5927'})),
    #  ('2020-03-05', (_20:company {company_name: '\u78a7\u6842\u56ed'}))]    
    
    last_company = None
    for time_and_company in time_and_company_list:
        this_company = time_and_company[1]
        employ_start = time_and_company[0]
        if not last_company:
            last_company = this_company
            continue
        new_connect_dict = {
            'start_company': last_company['company_name'],
            'end_company': this_company['company_name'],
            'employee': employee['employee_name'],          # 这个地方也可以是员工的id
            'employ_start': employ_start,
          }
        new_connect_list.append(new_connect_dict)
        last_company = this_company
    
pprint(new_connect_list)
##  [{'employ_start': '2020-03-05',
##    'employee': '张三',
##    'end_company': '碧桂园',
##    'start_company': '恒大'},
##   {'employ_start': '2020-03-01',
##    'employee': '李四',
##    'end_company': '恒大',
##    'start_company': '首创'}]
  

for new_connect in new_connect_list:
    cypher_match = "MATCH (start_node:company {company_name: \"%(start_company)s\"})"\
            "-[r:change_employ_to {employee:\"%(employee)s\", employ_start:"\
            "\"%(employ_start)s\"}]->(end_node:company {company_name: "\
            "\"%(end_company)s\"}) return start_node,r,end_node" % {
        'start_company': new_connect['start_company'],
        'end_company': new_connect['end_company'],
        'employee': new_connect['employee'],
        'employ_start': new_connect['employ_start']
      }
    match_result = test_graph.run(cypher_match)
    match_result = list(match_result)

    if len(match_result) == 0:
        cypher_create = "MATCH (start_node:company {company_name: \"%(start_company)s\"}), \
                (end_node:company {company_name: \"%(end_company)s\"}) CREATE \
                (start_node)-[r:change_employ_to {employee:\"%(employee)s\", \
                employ_start:\"%(employ_start)s\"}]->(end_node)" % {
            'start_company': new_connect['start_company'],
            'end_company': new_connect['end_company'],
            'employee': new_connect['employee'],
            'employ_start': new_connect['employ_start']
        }
        _ = test_graph.run(cypher_create)
    
    
    

##  查看某个人的流动路径
# employee_name = '张三'
employee_name = '李四'
cypher = "match (a:company)-[r:change_employ_to {employee:\"%s\"}]->\
    (b:company) return a,b,r" % employee_name
employee_path = test_graph.run(cypher)
employee_path = list(employee_path)
print(cypher)
print(employee_path)
    
    

