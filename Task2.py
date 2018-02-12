"""
下面的文件将会从csv文件中读取读取短信与电话记录，
你将在以后的课程中了解更多有关读取文件的知识。
"""
import csv
with open('texts.csv', 'r') as f:
    reader = csv.reader(f)
    texts = list(reader)

with open('calls.csv', 'r') as f:
    reader = csv.reader(f)
    calls = list(reader)

"""
任务2: 哪个电话号码的通话总时间最长? 不要忘记，用于接听电话的时间也是通话时间的一部分。
输出信息:
"<telephone number> spent the longest time, <total time> seconds, on the phone during
September 2016.".

提示: 建立一个字典，并以电话号码为键，通话总时长为值。
这有利于你编写一个以键值对为输入，并修改字典的函数。
如果键已经存在于字典内，为键所对应的值加上对应数值；
如果键不存在于字典内，将此键加入字典，并将它的值设为给定值。
"""
phone_time = {}
max_time = 0
max_phone = None

def check_phone_time(phone, call_times):
    """
    根据输入的电话号码和通话时长来变更统计字典。
    """
    if phone in phone_time:
        phone_time[phone] += int(call_times)
    else:
        phone_time[phone] = int(call_times)

#数据分析统计的主逻辑
for call in calls:
    check_phone_time(call[0], call[3])
    check_phone_time(call[1], call[3])

#根据数据分析的结果字典来进一步根据要求输出。
for phone in phone_time:
    if phone_time[phone] > max_time:
        max_time = phone_time[phone]
        max_phone = phone

print("{} spent the longest time, {} seconds, on the phone \
during September 2016.".format(max_phone, max_time))

