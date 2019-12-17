import json
import os
import re
import time
from functools import reduce
import datetime

import pymysql
import en_core_web_sm


import spacy
nlp = spacy.load('en_core_web_sm')

# nltk.download('punkt')
# date = 2019/12/10
# import srsly.msgpack.util
# import cymem.cymem
# import distutils.command.build_ext
# import preshed.maps

l = []

# *******************setting_re*****************start
# ************************定义中出现的固定字符，并生成正则表达式************************
# Acknowledgments = Acknowledgements 致谢
# Abstract = Overview 摘要=概要
# Bibliography = References 参考文献
# Appendix = appendices 目录、索引
# Preface 前言|序
# COPYRIGHT  版权  （这个字段匹配到的东西是需要删除的，问题是不知道截止到什么地方，！截止到  '.doc'的位置，london university）

# 对文章进行章节划分
"""
主要结构如下：（大小写、先后顺序每篇不太相同）-->（）可能存在，【】大部分存在，主要分段依据

--（COPYRIGHT(版权)）
-- Preface
-- 【title】
--（Declaration）
-- 【Acknowledgments | Acknowledgements（部分拼写不同）】
-- 【Abstract | Overview】
-- Introduction | GENERAL INTRODUCTION
-- 【Table of contents】
    -- List of Figures
    -- List of Tables
-- Contents
    -- Chapter
-- 【Bibliography | References】

"""
# 按照如下字段将文章进行分段
list_re_string = ['COPYRIGHT', 'Declaration', 'Acknowledgments', 'Acknowledgements',
                  'Abstract', 'Overview', 'Key words',
                  'Introduction', 'Contents', 'List of Figures', 'List of Tables',
                  'Nomenclature', 'Bibliography',
                  'References', 'Appendix', 'appendices', 'List of', 'Table of contents',
                  'Table', 'Chapter',
                  'GENERAL INTRODUCTION', 'Preface']

list_re_string_upper = list(map(lambda x: x.upper(), list_re_string))
re_string = '|'.join(list_re_string)
re_string_upper = '|'.join(list_re_string).upper()

# 主要分段依据的字段列表
list_main_part_keywords = ['Acknowledgments', 'Acknowledgements', 'Abstract', 'References', 'Bibliography']

# 如下字段为干扰内容（匹配到后忽略内容或清空内容）
list_rm_string = ['Nomenclature', 'List of Figures', 'List of Tables', 'Contents', 'Table of contents',
                  'Chapter']
list_rm_string_upper = list(map(lambda x: x.upper(), list_rm_string))

# 控制从文章第一段中能匹配到的信息的列名
# list_col = ['Name', 'name', 'Date', 'date', 'supervisor', 'supervisors', 'Supervised by']  # 控制从文章第一段中能匹配到的信息的列名
list_col = ['supervisor', 'supervisors', 'Supervised by', 'advisor']
re_string_name = '|'.join(list_col)

# 过滤掉参考文献相关的干扰数据
list_mid_char_len = ['Bibliography', 'References']  #
list_mid_char_len_upper = list(map(lambda x: x.upper(), list_mid_char_len))
str_mid_char_len = '|'.join(list_mid_char_len + list_mid_char_len_upper)
# 相关正则表达式
re_other = r'(%s|%s)[\n]*((.|\n)*?)(%s|%s)' % (
    re_string, re_string_upper, re_string, re_string_upper)  # 根据分段关键字，匹配其他各个段落
# re_other = r'(%s)[\n]*(.|\n)+'%(re_string)
# \u2018 = ’ \u2019 = '

# 过滤参考文献时不应该停止的字符列表
filter_references_list = ['COPYRIGHT', 'Declaration', 'Acknowledgments', 'Acknowledgements',
                          'Abstract', 'Overview', 'Key words','Keywords', 'KEYWORDS', 'KEY WORDS',
                          'Contents', 'List of Figures', 'List of Tables',
                          'Nomenclature', 'Bibliography',
                          'References', 'Appendix', 'appendices', 'List of', 'Table of contents',
                          'Table', 'Chapter',
                          ]
filter_references_list_upper = list(map(lambda x: x.upper(), filter_references_list))
filter_references_string = '|'.join(filter_references_list + filter_references_list_upper)


key_list = ['Acknowledgments', 'Acknowledgements',
                  'Abstract',
                  'Bibliography',
                  'References']


##################
# re_start = r'((.|\n)*?((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec))[.,]*\s\d{4})'
re_start = r'((.|\n)*?)(%s|%s)' % (re_string, re_string_upper)  # 匹配第一段内容
re_start_author = r'(%s)((.|\n)*?\.\w*)' % re_string_name  # 从第一段内容中匹配作者
re_start_supervisors = r''
########################


#########################
# 脏字符需要替换成空
list_invalid_symbol = ['\uffff', '"', '\u2026', '\u2018', '\u2019', '\t', '\u00a0', '..', "'", '\\', '\u2022']

# 没有内容的txt文本
list_no_words = []

# abstract_acknowlegement的最小长度不应该小于
abstract_acknowlegement_min_len = 200


# 数据库连接配置
# db_setting = {
#     'host': '127.0.0.1',
#     'user': 'root',
#     'password': '123',
#     'port': 3306,
#     'db': 'data',
# }

# 数据库表名
table_name = 'pdf2txt_local_temp_test'

# 解析文件夹路径
analysis_path = r'E:\work\CE'

# txt文件应该写在那个目录下

txt_file_path = os.path.dirname(os.path.abspath(__file__))  # 脚本临时运行下的文件夹
# *******************setting_re*****************end


# ****************************n_everyone.py**************start
# 定义没有关键字：by，需要删除前几行内容中存在的如下字符的行
list_by_keywords = ['NIVERSITY', 'COLLEGE', 'EDUCATION', 'GRADUATE', 'INSTITUTE', 'PSYCHOLOGY']


def rm_title_interference(e_title, by=False):
    '''
    提取title: 如果没有by， 删除第一段内容中的存在干扰字符的行
    :param e_title:
    :param by: 区分by是否存在的情况，现在找到前3行内容大部分会包含title，且会多出来一部分内容
    :return:
    '''
    e_title = e_title.strip()
    list_content = e_title.split(
        '\n')  # 如果没有by，按照\n分割，默认匹配前两行; 如果前两行有[NIVERSITY, COLLEGE, EDUCATION, SCHOOL]，删除相关的行，再往后匹配两行

    # 删除匹配到的字符，删除空、空格等干扰字符
    [list_content.remove(y) for x in list_by_keywords for y in list_content if
     x in y]
    [list_content.remove(x) for x in list_content if not x or x == ' ']
    if by:  # 区分有by的情况，但是现在有by和没有by情况一样
        e_title = ' '.join(list_content[:3]).strip()  # 存在干扰字符从后面再匹配3行
    else:
        e_title = ' '.join(list_content[:3]).strip()  # 存在干扰字符从后面再匹配3行
    return e_title

def Recommended_https(content, di_res, start_condition='Citation', end_condition='https'):
    start_condition = start_condition
    end_condition = end_condition
    content = content.split('Recommended')[-1]
    content = content.split(end_condition, 1)[0] + end_condition
    content = content.replace('\n', ' ')
    # e_title = r'''(%s)(.|\n)*?["':]((.|\n)*)?"(.|\n)*?(%s)''' % (start_condition, end_condition)
    e_title = r'''(%s)["':]*((.|\n)*)?["\(]*(%s)''' % (start_condition, end_condition)
    # e_title = r'(Recommended Citation)(.|\n)*?"((.|\n)*)?"(.|\n)*(https://lib.dr.iastate.edu/etd/)' % (start_condition, end_condition)
    try:
        e_title = re.search(e_title, content).group(2).strip()
        # print('=====e_title===========', e_title)

        di_res['TITTLE'] = e_title
        # print('---every_one_university----', di_res)
        return True
    except Exception as e:
        # print('=====title===========', e)
        return False


def every_one_university(*args, **kwargs):
    '''
    通用匹配的规则：
    :param args:
    :param kwargs:
    :return:
    '''
    content = args[0]
    organ_name = args[1]
    author_name = args[2]
    di_res = args[3]

    # 匹配tittle

    if organ_name == 'ndh':

        re_title = r'%s((.|\n)*?)(by|By|%s)' % ('eprints@nottingham.ac.uk', author_name)
        try:
            e_title = re.search(re_title, content).group(1)
            di_res['TITTLE'] = rm_enter_key(e_title)
            return
        except Exception as e:
            # print(e, '>>>>>> no ndh')
            pass

    # 路易斯
    elif organ_name == 'lsu':
        start_condition = 'Recommended Citation'
        end_condition = 'http://digitalcommons.lsu.edu/'
        re_name_title_doc_type = r'%s((.|\n)*)?"((.|\n)*?)(lsu|Lsu|LSU)?(.*)?%s' % (start_condition, end_condition)
        try:
            e_author_name = re.search(re_name_title_doc_type, content).group(1).strip(',')
            e_title = re.search(re_name_title_doc_type, content).group(2).strip()
            e_doc_type = re.search(re_name_title_doc_type, content).group(4).strip()
            di_res['NAME'] = e_author_name
            di_res['TITTLE'] = e_title
            di_res['DOC_TYPE'] = e_doc_type
            return
        except Exception as e:
            # print('>>>>>>  no lsu', e)
            pass

    #  Norwegian University of Science and Technology挪威科技
    elif organ_name == 'Norwegian University of Science and Technology':
        start_condition = 'Recommended Citation'
        end_condition = 'http://digitalcommons.lsu.edu/'
        re_name_title_doc_type = r'%s((.|\n)*)?"((.|\n)*?)(lsu|Lsu|LSU)?(.*)?%s' % (start_condition, end_condition)
        try:
            e_author_name = re.search(re_name_title_doc_type, content).group(1).strip(',')
            e_title = re.search(re_name_title_doc_type, content).group(2).strip()
            e_doc_type = re.search(re_name_title_doc_type, content).group(4).strip()
            di_res['NAME'] = e_author_name
            di_res['TITTLE'] = e_title
            di_res['DOC_TYPE'] = e_doc_type
            return
        except Exception as e:
            # print('>>>>>>  no lsu', e)
            pass

    elif organ_name == 'THE UNIVERSITY OF GHANA':
        re_memorial_start = r"""((.|\n)*?((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec)[.,]*\s\d{4})|\d{4})"""
        # re_memorial_start =r"""((.|\n)*?((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec)|(JAN|FEB|MAR|APR|AUG|SEPT|OCT|NOV|DEC)|(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER))[.,]*\s\d{4})"""
        try:
            e_start = re.match(re_memorial_start, content, re.I).group(1)
        except Exception as e:
            return

    # Iowa State University Capstones 爱荷华州立************
    elif organ_name == 'Iowa State University Capstones':
        content = content.split('Recommended')[-1]
        content = content.split('https://lib.dr.iastate.edu/etd/')[0] + 'https://lib.dr.iastate.edu/etd/'
        start_condition = 'Citation'
        end_condition = 'https://lib.dr.iastate.edu/etd/'
        # e_title = r'''(%s)(.|\n)*?["':]((.|\n)*)?"(.|\n)*?(%s)''' % (start_condition, end_condition)
        e_title = r'''(%s)(.|\n)*?["':]*((.|\n)*)?["\(]*(.|\n)*?(%s)''' % (start_condition, end_condition)
        # e_title = r'(Recommended Citation)(.|\n)*?"((.|\n)*)?"(.|\n)*(https://lib.dr.iastate.edu/etd/)' % (start_condition, end_condition)
        try:
            e_title = re.search(e_title, content).group(3).strip()
            # print('=====e_title===========', e_title)

            di_res['TITTLE'] = e_title
            # print('---every_one_university----', di_res)

            return
        except Exception as e:
            print('=====title===========', e)
            pass

    # Old Dominion University  老自治领大学????
    elif organ_name == 'Old Dominion University':
        content = content.split('Recommended')[-1]
        content = content.split('https://lib.dr.iastate.edu/etd/')[0] + 'https://lib.dr.iastate.edu/etd/'
        start_condition = 'Citation'
        end_condition = 'https://lib.dr.iastate.edu/etd/'
        # e_title = r'''(%s)(.|\n)*?["':]((.|\n)*)?"(.|\n)*?(%s)''' % (start_condition, end_condition)
        e_title = r'''(%s)(.|\n)*?["':]*((.|\n)*)?["\(]*(.|\n)*?(%s)''' % (start_condition, end_condition)
        # e_title = r'(Recommended Citation)(.|\n)*?"((.|\n)*)?"(.|\n)*(https://lib.dr.iastate.edu/etd/)' % (start_condition, end_condition)
        try:
            e_title = re.search(e_title, content).group(3).strip()
            # print('=====e_title===========', e_title)

            di_res['TITTLE'] = e_title
            # print('---every_one_university----', di_res)

            return
        except Exception as e:
            print('=====title===========', e)
            pass

    # Western Washington University     


    # Utah State University     
    elif organ_name == 'Utah State University':
        start_condition = 'Citation'
        end_condition = 'https://digitalcommons.usu.edu/honors/'
        content = content.split('Recommended')[-1]
        content = content.split(end_condition)[0] + end_condition

        # e_title = r'''(%s)(.|\n)*?["':]((.|\n)*)?"(.|\n)*?(%s)''' % (start_condition, end_condition)
        e_title = r'''(%s)(.|\n)*?["':]*((.|\n)*)?["\(]*(.|\n)*?(%s)''' % (start_condition, end_condition)
        # e_title = r'(Recommended Citation)(.|\n)*?"((.|\n)*)?"(.|\n)*(https://lib.dr.iastate.edu/etd/)' % (start_condition, end_condition)
        try:
            e_title = re.search(e_title, content).group(3).strip()
            # print('=====e_title===========', e_title)

            di_res['TITTLE'] = e_title
            # print('---every_one_university----', di_res)

            return
        except Exception as e:
            # print('=====title===========', e)
            pass

    # The Faculty of Humboldt State University  洪堡州立大学  
    # 标题中存在部分关键词，被删除


    # Missouri State University 密苏里州立大学
    start_condition = 'Citation'
    end_condition = 'https://bearworks.missouristate.edu/theses/'

    # University of Arkansas 阿肯色大学
    start_condition = 'Citation'
    end_condition = 'https://scholarworks.uark.edu/cveguht/'

    # Montclair State University  蒙特克莱尔州立大学
    # 'https://digitalcommons.montclair.edu/etd/'

    # Montclair State University  蒙特克莱尔州立大学
    ''
    # the University of Iowa  爱荷华大学
    # by之前，三行或者lib-ir@uiowa.edu.到by之间


    # 通用匹配

    # re_memorial_start =r"""((.|\n)*?((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec)|(JAN|FEB|MAR|APR|AUG|SEPT|OCT|NOV|DEC)|(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER))[.,]*\s\d{4})"""
    # try:
    #     e_start = re.match(re_memorial_start, content, re.I).group(1)
    res = Recommended_https(content, di_res)

    # re_memorial_start = r"""((.|\n)*?((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec)[.,]*\s\d{4})|\d{4})"""
    # try:
    #     e_start = re.match(re_memorial_start, content, re.I).group(1)
    # except Exception as e:
    #     write_file('content没有内容.txt', di_res.get('ID'))
    #     return
    try:
        e_by = re.search(r'by', e_start, re.I).group()
        e_title = re.search(r'((.|\n)*?)by', e_start, re.I).group(1)
        e_author_name = re.search(r'by(\s|\n)*(.*)', e_start, re.I).group(2)

        # 删除第一段内容中的存在干扰字符的行
        e_title = rm_title_interference(e_title, by=True)

        di_res['NAME'] = rm_enter_key(e_author_name)
        di_res['TITTLE'] = rm_enter_key(e_title)

        return

    except Exception as e:
        doc = nlp(e_start)
        for attribute in doc.ents:
            # print(attribute.text, attribute.label_)
            if attribute.label_ == 'PERSON':
                e_author_name = rm_enter_key(attribute.text)
                di_res['NAME'] = rm_enter_key(e_author_name)

            elif attribute.label_ == 'ORG':
                e_org_name = rm_enter_key(attribute.text)
                di_res['ORG'] = rm_enter_key(e_org_name)

            elif attribute.label_ == 'DATE':
                e_date = rm_enter_key(attribute.text)
                di_res['e_date'] = rm_enter_key(e_date)

        if author_name:
            re_title_author = '((.|\n)*)?%s' % author_name
            e_title = re.search(re_title_author, e_start).group()

            # 删除第一段内容中的存在干扰字符的行
            e_title = rm_title_interference(e_title)

        else:
            # 删除第一段内容中的存在干扰字符的行
            e_title = rm_title_interference(content)

        di_res['TITTLE'] = rm_enter_key(e_title)
        return
    except Exception as e:
        return
# ****************************n_nottingham_everyone.py**************end



def rm_invalid_symbol(content):
    '''
    删除脏字符|将脏字符转换成正常字符
    :param content:
    :return:
    '''
    for invalid_symbol in list_invalid_symbol:
        content = content.replace(invalid_symbol, '')
        # content = re.sub(r'\\u[A-Za-z0-9]{4}', '', content)
    content = content.replace('\ufb01', 'fi').replace('\ufb03', 'ffi').replace('\t \u00a0', '')
    # content = re.sub(r"([*-•-.>.v■>#,:^•^f;;,«»:.-/^$®♦]\s)","",content)
    return content


def rm_enter_key(value):
    '''
    删除换行符
    :param value:
    :return:
    '''
    value = value.replace('\n', '').replace('    ', '')
    return value


def write_file(file_name, content):
    with open(txt_file_path + os.path.sep + file_name, 'ab') as f:
        f.write((json.dumps(content) + '\r\n').encode('utf-8'))

#################************分割摘要和致谢
def abstract_segmentation(content, di_paragraph, di_res, re_start, re_end, n_name, save_success_key):
    '''
    首先对文章进行分段，改变di_paragraph和di_res， 返回清空部分内内容的content
    :param content:
    :return: content(剩下部分内容用于匹配开头的第一部分)
    '''
    count = 0  # 跳过目录中出现相应关键字，count对出现过的关键字进行统计计数
    # 将内容分为几个部分，按照：di_paragraph = {'ACKNOWLEDGEMENTS':'xxx', ...}存储,
    # 并过滤掉所有目录中的干扰字符
    # write_file('zhaiyao.txt', content)

    re_other = r'(%s)[\n]*((.|\n)*?)(%s|%s|%s)' % (
        re_start, re_end, re_end.upper(), re_end.capitalize())  # 根据分段关键字，匹配其他各个段落
    try:
        # val_start = re.search(re_start, content, re.I).group(1)
        val_start = ''
    except Exception as e:
        print('************第一段内容匹配失败************,', e)
        val_start = ''
    while True:
        try:
            val_sec = re.search(re_other, content)

            key = val_sec.group(1)
            value = val_sec.group(2)
            key_end_word = val_sec.group(4)

            if key_end_word != key and value and len(value.strip()) > abstract_acknowlegement_min_len:  # 再before_content中，里面有两个abstract，那么先跳过一个
                content = content.replace(key, '', 1)
                di_paragraph.setdefault(key.replace(' ', '').upper(), rm_enter_key(value))
                save_success_key.append(key.upper())
            else:
                content = content.replace(key, '', 1)
                continue

            di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
            # if key.upper() == 'ABSTRACT':

        except Exception as e:  # 在内容匹配不到后，break退出循环
            print('************将abstract|acknowlegements内容分段完成,退出************', e)
            key = ''
            content = content
            break
    return content, val_start, key


#################************分割参考文献
def reference_segmentation(content, di_paragraph, di_res, re_start, re_end, n_name, save_success_key):
    '''
    首先对文章进行分段，改变di_paragraph和di_res， 返回清空部分内内容的content
    :param content:
    :return: content(剩下部分内容用于匹配开头的第一部分)
    '''
    count = 0  # 跳过目录中出现相应关键字，count对出现过的关键字进行统计计数
    # 将内容分为几个部分，按照：di_paragraph = {'ACKNOWLEDGEMENTS':'xxx', ...}存储,
    # 并过滤掉所有目录中的干扰字符

    try:
        # val_start = re.search(re_start, content, re.I).group(1)
        val_start = ''
        re_other = r'(%s)[\n]*((.|\n)*?)(%s|%s|%s)' % (
            re_start, re_end, re_end.lower(), re_end.capitalize())  # 根据分段关键字，匹配其他各个段落
        while True:
            try:
                val_sec = re.search(re_other, content)

                key = val_sec.group(1)
                value = val_sec.group(2)
                content = content.replace(key, '', 1)
                di_paragraph.setdefault(key.upper(), rm_enter_key(value))
                di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
                # print(key,'0000----', value,'====================name_==', n_name)
                save_success_key.append(key.upper())
            except Exception as e:  # 在内容匹配不到后，break退出循环
                print(re_start, re_end, '************将reference内容分段完成,退出************', e)
                key = ''
                break
    except Exception as e:
        print('************第一段内容匹配失败************,', e)
        val_start = ''
        key = ''

    return content, val_start, key


#################************和下面一个函数配合找到目录中的关键词的结束词
def table_of_contents_extract(content, n_name):
    # CONTENT
    content = content.replace('\n', ' ').split(' ')
    contents = [i for i in content if i != '']
    if 'Contents' in contents:
        index_content = contents.index('Contents')
    elif 'CONTENTS' in contents:
        index_content = contents.index('CONTENTS')
    elif 'CONTENT' in contents:
        index_content = contents.index('CONTENT')
    else:
        index_content = ''
    try:
        list_for = []
        content_all = contents[index_content:]
        for con in content_all:
            forest = re.findall('[a-zA-Z]+', con)
            if len(forest) != 0 and forest[0] not in ('page', 'Page', 'PAGE') and not re.match('[vViIxX]+', forest[0]):
                list_for.append(forest[0])
        # print(list_for)
    except Exception as e:
        print('没有找到目录', e)
        # with open('no_tableOfContents.txt', 'wb', encoding='utf-8') as f:
        #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name, 'error_info': '没有找到目录'}) + '\r\n')
        write_file('没有目录.txt', {'file_name': analysis_path + os.path.sep + n_name, 'error_info': '没有找到目录'})
        list_for = []
    return list_for


#################************和上面函数配合，并将值{abstract.upper():value}形式存储
def find_end_word(list_for):
    list_for_end = ['Acknowledgments', 'Acknowledgements',
                    'Abstract', 'Summary',
                    'Overview', 'Key words',
                    'Bibliography',
                    'References']
    # 根据contents后面的第一个词的大写还是首字符大写的形式，判断目录中其他词应该优先匹配大写还是首字符大写
    contents_end_first_word = list_for[1]
    if re.match(r'[A-Z][a-z]+', contents_end_first_word):  # 优先匹配首字符大写的
        list_for_end = list_for_end + list(map(lambda x: x.upper(), list_for_end))
    elif re.match(r'[A-Z]+', contents_end_first_word):  # 优先匹配全大写的
        list_for_end = list(map(lambda x: x.upper(), list_for_end)) + list_for_end
    else:
        list_for_end = list(map(lambda x: x.upper(), list_for_end)) + list_for_end
    di = {}
    for key in list_for_end:
        try:
            end_word = list_for[list_for.index(key) + 1]
            if len(end_word) < 2:  # 找两个词进行组合 作为
                end_word = list_for[list_for.index(key) + 2] + ' ' + list_for[list_for.index(key) + 3]
            elif end_word in ('the', 'The', 'he', 'He'):
                end_word = list_for[list_for.index(key) + 1] + ' ' + list_for[list_for.index(key) + 2]
            di.setdefault(key.replace(' ', '').upper(), end_word)
        except ValueError as e:
            # print(str(e))
            pass
            # 没有再目录列表中和全文中没有找到

    return di


def part_end_words_extract(content, key, _end_words_list, table_of_content_end_word, table_of_content):
    part_end_words = re.search(
        r'(Table\s+of\s+Contents|Contents|TABLE\s+OF\s+CONTENTS|CONTENTS)[\s\n]*?(.|\n)*?(%s)(.|\n)*?(\n|\s|\.|\d)+[A-Za-z\s\.]+?([a-zA-Z\s]+[A-Za-z])(.|\n)*?(%s)' % (
            key, table_of_content_end_word),
        table_of_content, re.I)

    # 精确的取到段落结束的关键词
    try:
        value_part_end_word = part_end_words.group(6).split('\n')[0]
    except Exception as e:
        print('end 关键词', e)
        value_part_end_word = ''

    if value_part_end_word and key in ('Bibliography', 'References'):
        rm_table_of_content_contents = content.replace(table_of_content, '')
        count = rm_table_of_content_contents.count(value_part_end_word)
        value_part_end_word = 'end' if count == 0 else value_part_end_word  # else:部分不能是''
    _end_words_list.append((key, value_part_end_word))
    return content


def article_segmentation(content, di_paragraph, di_res, ):
    '''
    首先对文章进行分段，改变di_paragraph和di_res， 返回清空部分内内容的content
    :param content:
    :return: content(剩下部分内容用于匹配开头的第一部分)
    '''
    count = 0  # 跳过目录中出现相应关键字，count对出现过的关键字进行统计计数
    # 将内容分为几个部分，按照：di_paragraph = {'ACKNOWLEDGEMENTS':'xxx', ...}存储,
    # 并过滤掉所有目录中的干扰字符

    try:
        val_start = re.search(re_start, content, re.I).group(1)
        re_other = r'(%s|%s)[\n]*((.|\n)*?)(%s|%s)' % (
            re_string, re_string_upper, re_string, re_string_upper)  # 根据分段关键字，匹配其他各个段落
    except Exception as e:
        print('************第一段内容匹配失败************,', e)
        val_start = ''

    return content, val_start


def respectively_extract(n_name, val_start, content, di_paragraph, di_res):
    '''
    分别提取各个段落中的内容，保存在di_paragraph、di_res
    :param n_name:
    :param val_start:
    :param content:
    :param di_paragraph:
    :param di_res:
    :return: None
    '''
    # 对di_paragraph中存在的字段进行解析，如果没有解析到结果，不要再di_paragraph中存储
    # """
    for i_string in list_re_string_upper:
        if i_string in di_paragraph:
            while True:
                try:
                    content_start = re.search(re_start_author, di_paragraph[i_string], re.I)  # 从第一段内容中匹配作者name

                    start_key = content_start.group(1)
                    start_key_upper = start_key.upper()
                    start_val = content_start.group(2)

                    # 将di_paragraph中的supervisor的key统一
                    if 'SUPERVISOR' in start_key_upper:
                        start_key_upper = 'SUPERVISOR'
                    if 'Key words' in start_key_upper:
                        start_key_upper = 'KEY_WORDS'

                    # 例如：从Acknowledgments 中提取supervisors
                    if i_string in ['ACKNOWLEDGMENTS', 'ACKNOWLEDGEMENTS']:
                        content_supervisor = re.findall(re_start_author, di_paragraph[i_string], re.I)
                        start_supervisor_val = reduce(lambda x, y: x + '' + y, [i[1] for i in content_supervisor])

                        # 这块也可以进行封装
                        doc_key = nlp(start_supervisor_val)
                        for attribute_key in doc_key.ents:
                            # print(attribute.text, attribute.label_)
                            if attribute_key.label_ == 'PERSON':
                                # print(',,,,,,000....')
                                if start_key_upper in di_res:
                                    # 当di_res中有存在的导师key，说明已经至少有一个导师存在了，那么为了保证正确，所以暂时只是导入一个导师
                                    # di_res[start_key_upper] = rm_enter_key(di_res[start_key_upper] + ';' + attribute_key.text)
                                    pass
                                else:
                                    di_res[start_key_upper] = rm_enter_key(attribute_key.text)
                            else:
                                print('没有识别到person')
                        # 这块也可以进行封装

                    else:
                        di_res.setdefault(start_key_upper, rm_enter_key(start_val))

                    # 清空掉  di_paragraph[i_string]  用来退出当前循环
                    di_paragraph[i_string] = rm_enter_key(
                        di_paragraph[i_string].replace(start_key, '').replace(start_val, ''))
                except Exception as e:
                    print('************di_paragraph中存在的字段进行解析完毕,退出************', e)
                    break
    # """
    # 第一段中提取内容：degree、级别、作者名称、时间
    # 匹配doc_type 和degree是强关联的关系，不能这两个结果有差别

    list_doctor = ['degree of Doctor', 'Doctorate', 'doctor', 'Ph D', 'Doctor']  # 博士学位关键字
    str_doctor = '|'.join(list_doctor)

    list_master = ['Master of', 'M.S.', 'M.S']  # 硕士学位关键字
    str_master = 'Master of|M\.S\.|M\.S'  # 修改正则匹配

    re_doc_type = r'(%s)' % (str_doctor + '|' + str_master)  # 先匹配博士|再匹配硕士，证明博士优先与硕士

    try:
        val_doc_type = re.search(re_doc_type, content).group(1)
    except:
        val_doc_type = ''
    if val_doc_type in list_doctor:
        val_doc_type = 'PD'
    elif val_doc_type in list_master:
        val_doc_type = 'MD'
    elif not val_doc_type:
        val_doc_type = 'ET'

    di_res['doc_type'.upper()] = rm_enter_key(val_doc_type)

    # 匹配degree
    # re_degree = '(.*?(%s|%s))'%(re_string, re_string_upper)
    re_degree = '((Doctor\s+of\s+(.|\n)*?\w+)|(Master\s+of\s+(.|\n)*?\w+).*?[\.|\n])'
    try:
        val_degree = re.search(re_degree, content).group()
    except Exception as e:
        print(e)
        val_degree = ''
    di_res['degree'.upper()] = rm_enter_key('the Degree of ' + val_degree)

    # 专业
    list_major = ['Philosophy', 'Science', 'Genetics', ]  # 专业关键字
    # list_major_upper =
    str_major = '|'.join(list_major)
    str_major_upper = '|'.join(list_major).upper()
    try:
        val_major = re.search(r'(%s|%s)' % (str_major, str_major_upper), content).group()
    except Exception as e:
        print(e)
        val_major = ''

    di_res['major'.upper()] = rm_enter_key(val_major)
    di_res['id'.upper()] = n_name

    # 院系
    re_department = r'(Department\s+of\s+(.|\n)*?\w+.*?[\.|\n])'
    try:
        val_department = re.search(re_department, content).group()
    except Exception as e:
        val_department = ''
    if len(val_department) > 200:
        val_department = val_department[:200]
    di_res['department'.upper()] = rm_enter_key(val_department)

    # 使用en_core_web_sm命名实体识别
    import spacy
    from spacy import displacy
    from collections import Counter
    # import en_core_web_sm
    # nlp = en_core_web_sm.load()
    doc = nlp(val_start)
    for attribute in doc.ents:
        if attribute.label_ == 'PERSON':
            di_res['NAME'] = rm_enter_key(attribute.text)
        elif attribute.label_ == 'ORG':
            di_res['ORG'] = rm_enter_key(attribute.text)
        elif attribute.label_ == 'DATE':
            di_res['DATE'] = rm_enter_key(attribute.text)
        di_res['id'] = rm_enter_key(n_name)

    # 识别title
    author_name = di_res.get('NAME', '')
    re_title = r'((.|\n)*?)(((January|February|March|April|May|June|July|August|September|October|November|December)|(Jan|Feb|Mar|Apr|Aug|Sept|Oct|Nov|Dec))[.,]*\s\d{4}|%s)' % author_name

    try:
        value_title = re.match(re_title, content).group(1)
        # if len(value_title) > 200:
        #     value_title = value_title[:200]
    except Exception as e:
        value_title = ''

    di_res['TITTLE'] = rm_enter_key(value_title)

    # 分离出来年和月份
    # 年份
    value_date = di_res.get('DATE', '')
    if not value_date:
        val_year = 0
        val_mouth = ''
    re_year = r'\d{4}'
    try:
        val_year = re.search(re_year, value_date).group(0)
    except Exception as e:
        val_year = 0
    di_res['YEAR'] = val_year

    # 月份
    re_mouth = r'[A-Za-z]+'
    try:
        val_mouth = re.search(re_mouth, value_date).group(0)
    except Exception as e:
        val_mouth = ''
    di_res['MOUTH'] = rm_enter_key(val_mouth)


def analysis_pat(content, n_name, table_name, n_path):
    '''
    解析规则
    :param content: 解析的内容
    :param n_name: 文件name
    :param table_name: 数据库表名
    :return:
    '''
    # 最终结果存储结构
    di_paragraph = {}
    di_res = {}

    # 分割成两个部分，目录前和目录后
    save_success_key = []
    # re_table_content = r'((.|\n)*?)(Contents|CONTENTS)'
    # table_of_contents_before_content = re.search(re_table_content, content)

    if 'CONTENTS' in content:
        before_content, after_content = content.split('CONTENTS', 1)
        before_content += ' CONTENTS'
        after_content = 'CONTENTS ' + after_content
    elif 'Contents' in content:
        before_content, after_content = content.split('Contents', 1)
        before_content += ' Contents'
        after_content = 'Contents ' + after_content
    else:
        # before_content = after_content = content
        # raise Exception('文章里面不能用content识别目录，没有content关键字')
        # with open('目录中没有contents关键字.txt', 'ab', encoding='utf-8') as f:
        #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name, 'error_info': '文章里面不能用content识别目录，没有content关键字'}) + '\r\n')
        write_file('目录中没有contents关键字.txt',
                   {'file_name': analysis_path + os.path.sep + n_name,
                    'error_info': '文章里面不能用content识别目录，没有content关键字'}
                   )
        # return

    re_end = ['COPYRIGHT', 'Declaration', 'Acknowledgments', 'Acknowledgements',
              'Abstract', 'Overview', '(Key words)', 'Keywords', 'KEYWORDS', '(KEY WORDS)',
              'Contents', 'List of Figures', 'List of Tables',
              'Nomenclature', 'List of', '(Table of contents)',
              'Table', 'Chapter',
              ]
    re_end_str = '|'.join(re_end)
    re_end_str = re_end_str + '|' + re_end_str.upper()

    # ABSTRACT
    re_start_abstact = ['ABSTRACT', 'Abstract', 'A B S T R A C T', 'A B ST R A C T', 'SUMMARY', 'Summary']
    re_start_abstact_str = '|'.join(re_start_abstact)

    _res, _value, key = abstract_segmentation(before_content, di_paragraph, di_res, re_start_abstact_str, re_end_str,
                                              n_name, save_success_key)
    # with open('di_paragraph.txt', 'a', encoding='utf-8') as f:
    #     f.write(json.dumps(di_paragraph) + '\r\n')
    # write_file('di_paragraph.txt',
    #            di_paragraph
    #            )
    # if key.upper() == 'Abstract'.upper():
    if 'abstract'.upper() in save_success_key:
        pass

    else:
        # re_table_content = r'((Contents|CONTENTS)(.|\n)*)'
        # contents = re.search(re_table_content, content)

        list_for = table_of_contents_extract(content, n_name)
        di_end_words = find_end_word(list_for)
        if 'abstract'.upper() in di_end_words:
            abstract_end_word = di_end_words.get('abstract'.upper(), '')
            after_content = after_content.split(abstract_end_word, 1)[-1]
            abstract_segmentation(after_content, di_paragraph, di_res, re_start_abstact_str, abstract_end_word, n_name,
                                  save_success_key)
        else:
            # raise Exception('目录里面没有abstract')
            # with open('目录里面没有abstract.txt', 'ab', encoding='utf-8') as f:
            #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}) + '\r\n')

            write_file('目录里面没有abstract.txt',
                       {'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}
                       )

            # return
    # elif key.upper() == 'Summary'.upper():
    #     if 'Summary'.upper() in save_success_key:
    #         pass
    #     else:
    #         # re_table_content = r'((Contents|CONTENTS)(.|\n)*)'
    #         # contents = re.search(re_table_content, content)
    #         list_for = table_of_contents_extract(content, n_name)
    #         di_end_words = find_end_word(list_for)
    #         if 'Summary'.upper() in di_end_words:
    #             abstract_end_word = di_end_words.get('Summary'.upper(), '')
    #             after_content = after_content.split(abstract_end_word, 1)[-1]
    #
    #             abstract_segmentation(after_content, di_paragraph, di_res, re_start_abstact_str, abstract_end_word,
    #                                   n_name,
    #                                   save_success_key)
    #         else:
    #             # raise Exception('目录里面没有abstract')
    #             # with open('目录里面没有abstract.txt', 'ab', encoding='utf-8') as f:
    #             #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}) + '\r\n')
    #
    #             write_file('目录里面没有abstract.txt',
    #                        {'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}
    #                        )
    #
    #             return

    # acknowlegement
    re_start_acknowledgments = ['ACKNOWLEDGMENTS', 'ACKNOWLEDGEMENTS', 'Acknowledgments', 'Acknowledgements']
    re_start_acknowledgments_str = '|'.join(re_start_acknowledgments)

    _res, _value, key = abstract_segmentation(before_content, di_paragraph, di_res, re_start_acknowledgments_str,
                                              re_end_str, n_name,
                                              save_success_key)
    # with open('di_paragraph.txt', 'ab') as f:
    #     f.write(json.dumps(di_paragraph) + '\r\n')
    # write_file('di_paragraph.txt',
    #            di_paragraph
    #            )

    if key.upper() == 'Acknowledgments'.upper():

        if 'Acknowledgments'.upper() in save_success_key:
            pass
        else:
            # re_table_content = r'((Contents|CONTENTS)(.|\n)*)'
            # contents = re.search(re_table_content, content)
            list_for = table_of_contents_extract(content, n_name)
            di_end_words = find_end_word(list_for)
            if 'Acknowledgments'.upper() in di_end_words:
                abstract_end_word = di_end_words.get('Acknowledgments'.upper(), '')
                after_content = after_content.split(abstract_end_word, 1)[-1]

                abstract_segmentation(after_content, di_paragraph, di_res, re_start_abstact_str, abstract_end_word,
                                      n_name,
                                      save_success_key)
            else:
                # raise Exception('目录里面没有acknowlegement')
                # with open('error.txt', 'ab', encoding='utf-8') as f:
                #     f.write(json.dumps(
                #         {'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}) + '\r\n')
                # with open('目录中没有contents关键字.txt', 'ab', encoding='utf-8') as f:
                #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name,
                #                         'error_info': '目录里面没有abstract'}) + '\r\n')
                write_file('目录中没有contents关键字.txt',
                           {'file_name': analysis_path + os.path.sep + n_name,
                            'error_info': '目录里面没有abstract'}
                           )

                # return
    elif key.upper() == 'Acknowledgements'.upper():

        if 'Acknowledgements'.upper() in save_success_key:
            pass
        else:
            # re_table_content = r'((Contents|CONTENTS)(.|\n)*)'
            # contents = re.search(re_table_content, content)
            list_for = table_of_contents_extract(content, n_name)
            di_end_words = find_end_word(list_for)
            if 'Acknowledgements'.upper() in di_end_words:
                abstract_end_word = di_end_words.get('Acknowledgements'.upper(), '')
                after_content = after_content.split(abstract_end_word, 1)[-1]

                abstract_segmentation(after_content, di_paragraph, di_res, re_start_abstact_str, abstract_end_word,
                                      n_name,
                                      save_success_key)
            else:
                # raise Exception('目录里面没有acknowlegement')
                # with open('error.txt', 'ab', encoding='utf-8') as f:
                #     f.write(json.dumps(
                #         {'file_name': analysis_path + os.path.sep + n_name, 'error_info': '目录里面没有abstract'}) + '\r\n')
                # with open('目录中没有contents关键字.txt', 'ab', encoding='utf-8') as f:
                #     f.write(json.dumps({'file_name': analysis_path + os.path.sep + n_name,
                #                         'error_info': '目录里面没有abstract'}) + '\r\n')
                write_file('目录中没有contents关键字.txt',
                           {'file_name': analysis_path + os.path.sep + n_name,
                            'error_info': '目录里面没有abstract'}
                           )
                # return

    # 参考文献
    re_start_reference = ['BIBLIOGRAPHY', 'REFERENCES', 'Bibliography', 'References']
    re_start_reference_str = '|'.join(re_start_reference)
    list_for = table_of_contents_extract(content, n_name)
    di_end_words = find_end_word(list_for)
    if 'Bibliography'.upper() in di_end_words:
        abstract_end_word = di_end_words.get('Bibliography'.upper(), '')
        after_content = after_content.split(abstract_end_word, 1)[-1]
        if abstract_end_word not in after_content:  # 结束的关键字找不到，执行references-appendix这种策略
            try:
                re_other = r'(Bibliography|BIBLIOGRAPHY)[\n]*((.|\n)*?)(%s|%s)' % (
                    re_string, re_string_upper)  # 根据分段关键字，匹配其他各个段落
                val_sec = re.search(re_other, after_content)
                key = val_sec.group(1)
                value = val_sec.group(2)
                di_paragraph.setdefault(key.upper(), rm_enter_key(value))
                di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
            except Exception as e:  # 如果也没有找到references-appendix，那么就直接匹配到文章结束
                re_other = r'(Bibliography|BIBLIOGRAPHY)[\n]*((.|\n)*)'  # 根据分段关键字，匹配其他各个段落
                try:
                    val_sec = re.search(re_other, after_content)
                    key = val_sec.group(1)
                    value = val_sec.group(2)
                    di_paragraph.setdefault(key.upper(), rm_enter_key(value))
                    di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
                    # print(key, '-----bib----', value)
                except Exception as e:
                    # with open('没有Bibliography|BIBLIOGRAPHY关键字.txt', 'ab', encoding='utf-8') as f:
                    #     f.write(json.dumps(
                    #         {'file_name': analysis_path + os.path.sep + n_name,
                    #          'error_info': '除去目录，剩余内容没有找到Bibliography|BIBLIOGRAPHY关键字'}) + '\r\n')
                    write_file('没有Bibliography&&BIBLIOGRAPHY关键字.txt',
                               {'file_name': analysis_path + os.path.sep + n_name,
                                'error_info': '除去目录，剩余内容没有找到Bibliography|BIBLIOGRAPHY关键字'}
                               )

        else:
            # print('**************=======after_content========', after_content)
            _res, _value, key = reference_segmentation(after_content, di_paragraph, di_res, re_start_reference_str,
                                                       abstract_end_word, n_name,
                                                       save_success_key)

    elif 'References'.upper() in di_end_words:
        abstract_end_word = di_end_words.get('References'.upper(), '')
        after_content = after_content.split(abstract_end_word, 1)[-1]
        if abstract_end_word not in after_content:  # 结束的关键字找不到，执行references-appendix这种策略
            try:
                re_other = r'(REFERENCES&&References)[\n]*((.|\n)*?)(%s|%s)' % (
                    re_string, re_string_upper)  # 根据分段关键字，匹配其他各个段落
                val_sec = re.search(re_other, after_content)
                key = val_sec.group(1)
                value = val_sec.group(2)
                di_paragraph.setdefault(key.upper(), rm_enter_key(value))
                di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
            except Exception as e:  # 如果也没有找到references-appendix，那么就直接匹配到文章结束
                re_other = r'(%s|%s)[\n]*((.|\n)*)' % (
                    re_string, re_string_upper)  # 根据分段关键字，匹配其他各个段落
                try:
                    val_sec = re.search(re_other, after_content)
                    key = val_sec.group(1)
                    value = val_sec.group(2)
                    di_paragraph.setdefault(key.upper(), rm_enter_key(value))
                    di_paragraph.setdefault('pdf_id', rm_enter_key(n_name))
                except Exception as e:
                    # with open('error.txt', 'ab', encoding='utf-8') as f:
                    #     f.write(json.dumps(
                    #         {'file_name': analysis_path + os.path.sep + n_name,
                    #          'error_info': '除去目录，剩余内容没有找到REFERENCES|References关键字'}) + '\r\n')
                    # with open('没有REFERENCES|References关键字.txt', 'ab', encoding='utf-8') as f:
                    #     f.write(json.dumps(
                    #         {'file_name': analysis_path + os.path.sep + n_name,
                    #          'error_info': '除去目录，剩余内容没有找到REFERENCES|References关键字'}) + '\r\n')
                    write_file('没有REFERENCES&&References关键字.txt',
                               {'file_name': analysis_path + os.path.sep + n_name,
                                'error_info': '除去目录，剩余内容没有找到REFERENCES|References关键字'}
                               )

        else:
            _res, _value, key = reference_segmentation(after_content, di_paragraph, di_res, re_start_reference_str,
                                                       abstract_end_word, n_name,
                                                       save_success_key)
    else:
        # raise Exception('目录里面没有References')
        # with open('目录里面没有References.txt', 'ab', encoding='utf-8') as f:
        #     f.write(json.dumps(
        #         {'file_name': analysis_path + os.path.sep + n_name,
        #          'error_info': '目录里面没有References'}) + '\r\n')
        write_file('目录里面没有References.txt',
                   {'file_name': analysis_path + os.path.sep + n_name,
                    'error_info': '目录里面没有References'}
                   )

    # with open('di_paragraph.txt', 'a', encoding='utf-8') as f:
    #     f.write(json.dumps(di_paragraph) + '\r\n')
    # write_file('di_paragraph.txt',
    #            di_paragraph
    #            )

    """        """
    # 对文章进行分段
    n_content, val_start = article_segmentation(content=content, di_paragraph=di_paragraph, di_res=di_res)

    # 分别提取各个段落中的内容
    respectively_extract(n_name, val_start, content, di_paragraph, di_res)

    # 将临时的结果写入文件：可以删除
    # with open('res.txt', 'a', encoding='utf-8') as f:
    #     f.write(json.dumps(di_paragraph) + '\r\n')
    #     f.write(json.dumps(di_res) + '\r\n')
    # with open('val_start.txt', 'a', encoding='utf-8') as f:
    #     f.write(json.dumps(val_start) + '%s' % n_name + '\r\n')

    references = di_paragraph.get('BIBLIOGRAPHY')
    if not references:
        references = di_paragraph.get('REFERENCES', '')



    author_name = di_res.get('NAME')[:100] if len(di_res.get('NAME', '')) > 100 else di_res.get('NAME', '')
    organ_name = '' if len(di_res.get('ORG', '')) > 200 else di_res.get('ORG', '')
    supervisor = '' if len(di_res.get('SUPERVISOR', '')) > 200 else di_res.get('SUPERVISOR', '')
    publish_time = '' if len(di_res.get('DATE', '')) > 200 else di_res.get('DATE', '')

    
    
    # 引入通用匹配规则模板
    # from nottingham_dir.n_nottingham_everyone import every_one_university
    organ_name = 'xxxxxxxxx'
    every_one_university(content, organ_name, author_name, di_res)
    n_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 将数据插入到数据库中
    # name之所以置空，是因为scapy没有识别出来人名，他返回原有字符串，插入时：error：data long
    # publist_time之所以置空，是因为scapy没有识别出来人名，他返回原有字符串，插入时：error：data long
    # organ_name之所以置空，是因为scapy没有识别出来地名，他返回原有字符串，插入时：error：data long
    # organ_name之所以置空，是因为scapy没有识别出来人名，他返回原有字符串，插入时：error：data long

    title = di_res.get('TITTLE', '')[:500] if len(di_res.get('TITTLE', '')) > 500 else di_res.get('TITTLE', '')

    pdf_id = di_res.get('ID', '').rstrip('.pdf')
    try:
        insert_sql = '''insert into %s (title, author, organ_name, publish_time, v_year, v_month, reference, doc_type, tutor, description, degree, major, department, pdf_id, subject, n_path, n_time) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s", "%s", "%s", "%s");''' \
                     % (table_name, title, author_name, organ_name, publish_time, di_res.get('YEAR', 0),
                        di_res.get('MOUTH', ''),
                        references[:65000] if references else '',
                        di_res.get('DOC_TYPE', ''), supervisor,
                        di_paragraph.get('ABSTRACT', '')[:65000] if di_paragraph.get('ABSTRACT', '') else '',
                        di_res.get('DEGREE', ''), di_res.get('MAJOR', ''), di_res.get('DEPARTMENT', ''), pdf_id,
                        di_res.get('KEY_WORDS', ''), n_path.replace('\\', '/'), n_time)
        cursor.execute(insert_sql)
        conn.commit()
    except Exception as e:  # 将没有插入到数据库的【文件名和报错信息】保存到文件中
        # with open('error_data_insert_into_mysql.txt', 'w', encoding='utf-8') as f:
        #     f.write(json.dumps({'file_name': pdf_id, 'error_info': str(e)}) + '\r\n')
        write_file('error_data_insert_into_mysql.txt',
                   {'file_name': pdf_id, 'error_info': str(e)}
                   )


def analysis_txt(path, table_name):
    '''
    解析txt文件
    :param path: txt文件存在的文件夹路径
    :param table_name: 数据库表name
    :return:
    '''
    for root, dir, files in os.walk(path):
        for name in files:
            file_path = os.path.join(root, name)
            if name in list_no_words:
                continue

            if ret1 and name.replace('.txt', '') in ret1:
                print('已存在************%s'%file_path)
                continue

            if name.endswith('.txt'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        n_name = f.name
                        content = f.read()

                except Exception as e:
                    write_file('xxxx.txt', name+'\n')
                    continue

                n_name = name.replace('txt', 'pdf')

                content = rm_invalid_symbol(content)
                try:
                    analysis_pat(content, n_name, table_name, path)
                except Exception as e:
                    write_file('跳过的文本文件.txt',
                               {'file_path': analysis_path + os.path.sep + path + n_name, 'error_info': str(e)})
                # break


if __name__ == '__main__':
    # 删除目录下临时生成.txt文件
    # root_path = os.path.abspath(os.path.dirname(__file__))
    # [os.remove(root_path + os.sep + file_name) for file_name in os.listdir(root_path) if file_name.endswith('.txt')]

    start_time = time.time()

    # 数据库连接
    conn = pymysql.connect(**db_setting)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)  # 返回字典数据类型
    select_pdf_id_sql = '''SELECT pdf_id FROM %s;'''% (table_name)
    cursor.execute(select_pdf_id_sql)
    ret1 = cursor.fetchall()  # 取全部
    ret1 = list(map(lambda x:x.get('pdf_id'), ret1))

    # # 先清空数据表
    # clear_table_sql = '''
    #                 delete from %s
    #             ''' % (table_name)
    #
    # cursor.execute(clear_table_sql)


# **********正式执行脚本取所有文件************start*****
#     for analysis_path in txt_dirs:
#         for root, dirs, filename in os.walk(analysis_path):
#             for dir in dirs:
#                 # if dir == '14.139.116.8':
#                 #     continue
#                 if os.path.isdir(root + os.path.sep + dir):
#                     n_path = root + os.path.sep + dir
#                     print('********%s开始********  %s' % (n_path, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
#                     analysis_txt(n_path, table_name)
#                     print('********%s结束********  %s' % (n_path, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
#                 # break
# **********正式执行脚本取所有文件************end******
    analysis_txt(analysis_path, table_name)

    cursor.close()
    conn.close()

    totul_time = time.time() - start_time
    print(totul_time)
# pyinstaller -F *.py
