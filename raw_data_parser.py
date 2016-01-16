"""
Process the raw data that downloaded from TREC API
raw data format:
MB111 Q0 309679431390203905 1 9.131583 UDel
# TResult(id:309679431390203905, rsv:9.131583213806152, screen_name:sadhakm, epoch:1362668304, text:RT @Devinder_Sharma: Just think. Where do water tankers get water from if there is acute shortage of water in a drought affected area?, followers_count:479, statuses_count:5118, lang:null, in_reply_to_status_id:0, in_reply_to_user_id:0, retweeted_status_id:309188615290908673, retweeted_user_id:92773212, retweeted_count:0)
MB111 Q0 308328643183116288 2 9.131583 UDel
# TResult(id:308328643183116288, rsv:9.131583213806152, screen_name:mirandamld, epoch:1362346251, text:Water has become a dominant theme in movies. Rango is about a water shortage. "He who controls the water, controls everything.", followers_count:504, statuses_count:44, lang:null, in_reply_to_status_id:0, in_reply_to_user_id:0, retweeted_status_id:0, retweeted_user_id:0, retweeted_count:0)
...
...
"""

import os,sys
import re
import json
import codecs

reload(sys)
sys.setdefaultencoding('utf-8')

def parse(input_path, output_folder):
    if not os.path.exists(input_path):
        print 'Input file:'+input_path+' DOES NOT EXIST!... exit ...'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    all_data = {}
    pattern1 = re.compile(r'MB(.)')
    pattern2 = re.compile(r'# TResult\((.*)\)')
    pattern3 = re.compile(r'id:(?P<id>.*), rsv:(?P<rsv>.*), screen_name:(?P<screen_name>.*), epoch:(?P<epoch>.*), text:(?P<text>.*), followers_count:(?P<followers_count>.*), statuses_count:(?P<statuses_count>.*), lang:(?P<lang>.*), in_reply_to_status_id:(?P<in_reply_to_status_id>.*), in_reply_to_user_id:(?P<in_reply_to_user_id>.*), retweeted_status_id:(?P<retweeted_status_id>.*), retweeted_user_id:(?P<retweeted_user_id>.*), retweeted_count:(?P<retweeted_count>.*)')
    keys = ['id', 'rsv', 'screen_name', 'epoch', 'text', 'followers_count', 
        'statuses_count', 'lang', 'in_reply_to_status_id', 'in_reply_to_user_id', 
        'retweeted_status_id', 'retweeted_user_id', 'retweeted_count']
    with codecs.open(input_path, 'rb', 'utf-8') as f:
        cur_qid = ''
        for line in f:
            line = line.strip()
            if pattern1.match(line):
                cur_qid = line.split()[0]
                if cur_qid not in all_data:
                    all_data[cur_qid] = []
            elif pattern2.match(line):
                m2 = pattern2.match(line)
                m = pattern3.match(m2.group(1))
                doc = {}
                success = True
                for k in keys:
                    try:
                        doc[k] = m.group(k)
                    except:
                        success = False
                        print line
                        print '-'*30
                        break
                if success:
                    all_data[cur_qid].append(doc)
    for qid in all_data:
        with codecs.open( os.path.join(output_folder, qid), 'wb', 'utf-8' ) as f:
            json.dump(all_data[qid], f)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Please provide the input_path... exit...'
        exit()
    input_path = sys.argv[1]
    if len(sys.argv) > 2:
        output_folder = sys.argv[2]
    else:
        output_folder = input_path+'_output'
    parse(input_path, output_folder)

