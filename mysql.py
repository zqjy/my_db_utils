# -*- coding: utf-8 -*-
import copy
import re
import time
import typing

import pymysql


class MySqlDBClass(object):
    """
    mysql操作类
    """

    def __init__(self, host: str, port: int, user: str, password: str, db: str, charset: str = 'utf8',
                 commit_num: int = 1000, cursor_str: str = 'dict') -> None:
        _cursor_dict = {
            'tuple': pymysql.cursors.Cursor,
            'tuple_ss': pymysql.cursors.SSCursor,
            'dict': pymysql.cursors.DictCursor,
            'dict_ss': pymysql.cursors.SSDictCursor,
        }
        if cursor_str not in _cursor_dict:
            raise Exception(f"参数cursor_str错误，仅支持选项：{', '.join(_cursor_dict)}")
        # 创建连接
        self.conn = pymysql.Connection(host=host, port=port, user=user, password=password, db=db, charset=charset,
                                       cursorclass=_cursor_dict[cursor_str])
        print(f"创建连接：地址：{host} 端口：{port} 用户名：{user} 数据库：{db}")
        self.cursor = self.conn.cursor()  # 获取游标
        self.cache_dict = {
            # 'insert': dict(),  # 增 缓存 字典
            # 'delete': dict(),  # 删 缓存 字典
            # 'update': dict(),  # 改 缓存 字典
            # 'select': dict(),  # 查 缓存 字典
        }
        self.count_dict = {
            'insert': 0,  # 增 数据统计
            'delete': 0,  # 删 数据统计
            'update': 0,  # 改 数据统计
            'select': 0,  # 查 数据统计
        }
        self.commit_num = commit_num  # 插入数据 提交数量

    def get_insert(self, table: str, data_dict: typing.Dict, key_type_dict: typing.Dict) -> str:
        """
        获取插入语句
        :param data_dict: 数据字典
        :param key_type_dict: 要新增的数据字段及类型
        :param table: 表名
        :return:
        """
        _item_key_list = []  # 新增数据字段列表
        _item_value_list = []  # 新增数据字段对应数据列表
        # 要新增的数据字段及类型
        for _key, _type in key_type_dict.items():
            if _key not in data_dict:
                continue
            _str = self.item_data_2_str(data=data_dict[_key], value_type=_type)
            if _str is None or _str == 'None':
                continue
            _item_key_list.append(f"`{_key}`")  # 添加键
            _item_value_list.append(f"{_str}")

        # 拼接语句
        ret_sql = f"INSERT INTO {self._name_str(d=table)} ({', '.join(_item_key_list)}) VALUES ({', '.join(_item_value_list)})"
        # print(f"查询语句：{ret_sql}")  # 调试
        return ret_sql  # 返回

    @staticmethod
    def get_delete(table: str, condition: str) -> str:
        """
        获取删除语句
        :param table: 表名
        :param condition: 条件
        :return:
        """
        # 拼接修改语句
        ret_sql = f"DELETE FROM {self._name_str(d=table)} WHERE {condition}"
        # print(f"删除语句：{ret_sql}")  # 调试
        return ret_sql  # 返回

    def get_update(self, table: str, condition: str, update_dict: typing.Dict, update_key_dict: typing.Dict) -> str:
        """
        获取修改语句
        :param update_dict: 要修改的字段数据
        :param update_key_dict: 要修改的字段键-类型 {'key': 'str'}
        :param table: 表名
        :param condition: 条件
        :return:
        """
        _update_list = []  # 要修改的数据字符串列表
        # 遍历字段数据
        for _key, _type in update_key_dict.items():
            if _key in update_dict:
                _value = update_dict[_key]  # 获取数据类型
                _str = self.item_data_2_str(data=_value, value_type=_type)
                _update_list.append(f"`{_key}` = {_str}")  # 添加字符串

        # 拼接修改语句
        ret_sql = f"UPDATE {self._name_str(d=table)} SET {', '.join(_update_list)} WHERE {condition}"
        # print(f"查询语句：{ret_sql}")  # 调试
        return ret_sql  # 返回

    def get_select(self, table: str, condition: str = r'1=1', item_key: str = r'*', start: int = 0,
                   step: int = 1000) -> str:
        """
        获取查询语句
        :param item_key: 数据展示字段
        :param table: 表名
        :param condition: 条件
        :param start: 分页开始位置
        :param step: 分页步长
        :return:
        """
        _limit = f"{start}, {step}"  # 拼接数据范围
        # 拼接查询语句
        ret_sql = f"SELECT {item_key} FROM {self._name_str(d=table)} WHERE {condition} LIMIT {_limit}"
        # print(f"查询语句：{ret_sql}")  # 调试
        return ret_sql  # 返回

    def insert(self, insert_dict: typing.Dict, unique_tuple: typing.Tuple = (), print_sql: bool = False,
               return_id: bool = False, update: bool = False) -> typing.Any:
        """
        插入方法
        :param update: 重复是否更新
        :param unique_tuple: 查重字段
        :param return_id: 是否返回数据
        :param insert_dict: 插入参数字典
            {
                'table': '',
                'data_dict': {},
                'key_type_dict': {},
            }
        :param print_sql: 是否打印sql语句
        :return:
        """
        # 查询数据是否存在
        _time_start = time.time()
        # 创建查询字典
        _select_dict = copy.deepcopy(insert_dict)
        # 是否有指定查重字段
        if unique_tuple:
            _select_dict['data_dict'] = {_k: _v for _k, _v in insert_dict['data_dict'].items() if _k in unique_tuple}
        _results_select = self.select_by_dict(**_select_dict, print_sql=print_sql)
        _time_end = time.time()
        _select_diff_time = _time_end - _time_start
        print(f"查重时间：{_select_diff_time} 秒")
        _results = 0
        if not _results_select:
            # 获取添加语句
            _sql = self.get_insert(**insert_dict)
            _cache_sql = self.get_insert(**_select_dict)  # 仅剩查重字段的插入语句
            ret_id = self._cache_execute(sql=_sql, cache_sql=_cache_sql, exe_type='insert', print_sql=print_sql)
        else:
            # 已有数据的id
            ret_id = _results_select[0]['id']
            print(f"数据已存在， id：{ret_id}")
            # 重复是否更新
            if update:
                _update_dict = {
                    'table': insert_dict['table'],
                    'condition': f"`id` = {ret_id}",
                    'update_dict': insert_dict['data_dict'],
                    'update_key_dict': insert_dict['key_type_dict'],
                }
                self.update(update_dict=_update_dict, print_sql=print_sql)

        # 判断是否返回id
        if return_id:
            return ret_id

    def delete(self, delete_dict: typing.Dict, print_sql: bool = False) -> int:
        """
        更新方法
        :param delete_dict: 更新参数字典
            {
                'table': '',
                'condition': '',
            }
        :param print_sql: 是否打印sql语句
        :return:
        """
        _results_select = self.select(select_dict=copy.deepcopy(delete_dict), print_sql=print_sql)
        _results = 0  # 默认返回值
        if len(_results_select) > 0:
            # 获取修改语句
            _sql = self.get_delete(**delete_dict)
            # 执行语句
            _results = self._cache_execute(sql=_sql, print_sql=print_sql, exe_type='delete')
            print(f"删除条数：{_results}")
        else:
            print(f"数据不存在 {delete_dict}")
        return _results  # 返回

    def update(self, update_dict: typing.Dict, print_sql: bool = False) -> int:
        """
        更新方法
        :param update_dict: 更新参数字典
            {
                'table': '',
                'condition': '',
                'update_dict': '',
                'update_key_dict': '',
            }
        :param print_sql: 是否打印sql语句
        :return:
        """
        _select_dict = {
            'table': update_dict['table'],
            'condition': update_dict['condition'],
        }
        _results_select = self.select(select_dict=_select_dict, print_sql=print_sql)
        _results = 0  # 默认返回值
        if len(_results_select) > 0:
            # 获取修改语句
            _sql = self.get_update(**update_dict)
            # 执行语句
            _results = self._cache_execute(sql=_sql, print_sql=print_sql, exe_type='update')
            print(f"修改条数：{_results}")
        else:
            print(f"数据不存在 {_select_dict}")
        return _results  # 返回

    def select(self, select_dict: typing.Dict, print_sql: bool = False) -> typing.List:
        """
        查询方法
        :param select_dict: 查询参数字典
            {
                'table': r'dd_college_specials',  # 表名
                'item_key': r'`id` AS `大学专业ID`',  # 输出字段键
                'condition': '',
                'start': 0, # 默认值
                'step': 1000,  # 默认值
            }
        :param print_sql: 是否打印sql语句
        :return:
        """
        _start, _step = self._get_start_step(select_dict=select_dict)
        select_dict['step'] = _step  # 回填步长
        ret_data_list = []  # 返回的数据列表
        while True:
            select_dict['start'] = _start  # 更新开始行
            # 获取查询语句
            _sql = self.get_select(**select_dict)
            # 执行语句
            _results = self._cache_execute(sql=_sql, print_sql=print_sql)
            # 遍历数据
            for _result in _results:
                ret_data_list.append(self._data_dict_cleaning(data_dict=_result))
            # 如果没有结果 终止
            if not _results or len(_results) < _step:
                break
            else:
                _start += _step

        return ret_data_list  # 返回

    def select_yield(self, select_dict: typing.Dict, print_sql: bool = False, is_debug=False) -> typing.Generator:
        """
        查询方法
        :param is_debug: 是否调试
        :param select_dict: 查询参数字典
            {
                'table': r'dd_college_specials',  # 表名
                'item_key': r'`id` AS `大学专业ID`',  # 输出字段键
                'condition': '',
                'start': 0, # 默认值
                'step': 1000,  # 默认值
            }
        :param print_sql: 是否打印sql语句
        :return:
        """
        _start, _step = self._get_start_step(select_dict=select_dict)
        if is_debug:
            print(f"数据获取调试")
            _step = _count = 1
        else:
            _count = self.select_count(select_dict=select_dict, print_sql=print_sql)  # 数据总数
        select_dict['step'] = _step  # 回填步长
        while _start < _count:
            select_dict['start'] = _start  # 更新开始行
            # 获取查询语句
            _sql = self.get_select(**select_dict)
            # 执行语句
            _results = self._cache_execute(sql=_sql, print_sql=print_sql)
            # 遍历数据
            for _result in _results:
                yield self._data_dict_cleaning(data_dict=_result)
            _start += _step  # 更新分页开始行
        print(f"数据获取完毕")

    def select_count(self, select_dict: typing.Dict, print_sql: bool = False) -> int:
        """
        获取数据总数
        :param select_dict: 查询参数字典
        :param print_sql: 是否打印sql语句
        :return:
        """
        _start, _step = self._get_start_step(select_dict=select_dict)
        select_dict['step'] = _step  # 回填步长
        # 获取数据总长度
        _count_select_dict = {
            'item_key': 'COUNT(*) as count',
            'table': select_dict['table'],
            'condition': select_dict.get('condition', '1=1'),
            'step': _step,
            'start': _start,
        }
        # 获取语句
        _sql = self.get_select(**_count_select_dict)
        # 执行语句
        _results = self._cache_execute(sql=_sql, print_sql=print_sql)
        return _results[0]  # 数据总数

    def select_by_dict(self, table: str, data_dict: typing.Dict, key_type_dict: typing.Dict,
                       print_sql: bool = False) -> typing.List:
        """
        根据数据查询结果
        :param print_sql: 是否打印sql
        :param table: 表名
        :param data_dict: 数据字典
        :param key_type_dict: 有效键-类型字典
        :return:
        """
        _item_key_value_list = []  # 字段键值对列表
        # 遍历有效键-类型字典 获取查询条件
        for _key, _type in key_type_dict.items():
            if _key not in data_dict:
                continue
            _value = data_dict[_key]  # 数据
            _value = self.item_data_2_str(data=_value, value_type=_type)  # 数据根据类型转字符串
            # if re.match(r'str', _type):
            if _type == str:
                _key_value_str = f"`{_key}` LIKE {_value}"
            elif _value == 'None' or _value is None:
                _key_value_str = f""
            else:
                _key_value_str = f"`{_key}` = {_value}"
            if _key_value_str:
                _item_key_value_list.append(_key_value_str)  # 追加
        # print(_item_key_value_list)
        _condition = ' AND '.join(_item_key_value_list)  # 查询条件
        # print(f"条件：{_condition}")  # 调试
        # 获取查询语句
        _select_dict = {
            'table': table,
            'condition': _condition,
        }
        # 查询
        return self.select(_select_dict, print_sql=print_sql)

    def select_table_info(self, name: str, schema: str = '') -> typing.List:
        """
        获取表结构详细信息
            TABLE_CATALOG 表限定符 永远是def
            TABLE_SCHEMA 表格所属的库
            TABLE_NAME 表名
            COLUMN_NAME 字段名
            ORDINAL_POSITION 字段标识 其实就是字段编号，从1开始往后排
            COLUMN_DEFAULT 字段默认值
            IS_NULLABLE 字段是否可以是NULL 该列记录的值是YES或者NO
            DATA_TYPE 数据类型 里面的值是字符串，比如varchar，float，int
            CHARACTER_MAXIMUM_LENGTH 字段的最大字符数
                假如字段设置为varchar(50)，那么这一列记录的值就是50
                该列只适用于二进制数据，字符，文本，图像数据。其他类型数据比如int，float，datetime等，在该列显示为NULL
            CHARACTER_OCTET_LENGTH 字段的最大字节数
                和最大字符数一样，只适用于二进制数据，字符，文本，图像数据，其他类型显示为NULL
                和最大字符数的数值有比例关系，和字符集有关。比如UTF8类型的表，最大字节数就是最大字符数的3倍
            NUMERIC_PRECISION 数字精度
                适用于各种数字类型比如int，float之类的
                如果字段设置为int(10)，那么在该列保存的数值是9，少一位，还没有研究原因
                如果字段设置为float(10,3)，那么在该列报错的数值是10
                非数字类型显示为在该列NULL
            NUMERIC_SCALE 小数位数
                和数字精度一样，适用于各种数字类型比如int，float之类
                如果字段设置为int(10)，那么在该列保存的数值是0，代表没有小数
                如果字段设置为float(10,3)，那么在该列报错的数值是3
                非数字类型显示为在该列NULL
            DATETIME_PRECISION
                datetime类型和SQL-92interval类型数据库的子类型代码
                我本地datetime类型的字段在该列显示为0
                其他类型显示为NULL
            CHARACTER_SET_NAME 字段字符集名称 比如utf8
            COLLATION_NAME 字符集排序规则
                比如utf8_general_ci，是不区分大小写一种排序规则。utf8_general_cs，是区分大小写的排序规则。
            COLUMN_TYPE 字段类型 比如float(9,3)，varchar(50)
            COLUMN_KEY 索引类型
                可包含的值有PRI，代表主键，UNI，代表唯一键，MUL，可重复
            EXTRA 其他信息 比如主键的auto_increment。
            PRIVILEGES 权限
                多个权限用逗号隔开，比如 select,insert,update,references
            COLUMN_COMMENT 字段注释
            GENERATION_EXPRESSION 组合字段的公式
        :param name: 表名
        :param schema: 数据库名
        :return:
        """
        # 数据库名称初始化
        if not schema:
            schema = str(self.conn.db, encoding='utf-8')
        # 查询字典
        _sql_dict = {
            'table': r'information_schema.columns',  # 表名
            'item_key': self._item_str(item_dict={
                'COLUMN_NAME': 'name',
                'COLUMN_DEFAULT': 'default',
                'IS_NULLABLE': 'is_null',
                'DATA_TYPE': 'type',
                'COLUMN_TYPE': 'col_type',
                'COLUMN_KEY': 'key',
                'COLUMN_COMMENT': 'commit',
            }),  # 输出字段键
            'condition': self._and(
                c=[
                    self._equal(k='table_schema', v=schema),
                    self._equal(k='table_name', v=name),
                ]
            ),
        }
        return self.select(select_dict=_sql_dict, print_sql=True)

    def desc_table(self, table, print_sql: bool = False) -> typing.List:
        """
        获取表结构
            Field:字段表示的是列名
            Type:字段表示的是列的数据类型
            Null :字段表示这个列是否能取空值
            Key :在mysql中key 和index 是一样的意思，这个Key列可能会看到有如下的值：PRI(主键)、MUL(普通的b-tree索引)、UNI(唯一索引)
            Default: 列的默认值
            Extra :其它信息
        :param print_sql: 是否打印输出语句
        :param table: 表名
        :return:
        """
        # 查询语句
        _sql = f"DESC {self._name_str(d=table)}"
        # 执行
        return self._cache_execute(sql=_sql, print_sql=print_sql)

    def _item_str(self, item_dict: typing.Dict = None, alias: str = '') -> str:
        """
        获取输出字段字符串
        :param item_dict: 字段字典
        :param alias: 别名
        :return:
        """
        # 重置别名
        if alias:
            alias += '.'
            ret_str = f"{alias}*"
        else:
            ret_str = '*'

        if item_dict:
            ret_str = ', '.join([self._alias(d=f"{alias}{_k}", a=_v)for _k, _v in item_dict.items()])

        return ret_str

    def _alias(self, d: str, a: str):
        """
        别名
        :param d: 数据源
        :param a: 别名
        :return:
        """
        return f"{self._name_str(d=d)} AS `{a}`" if a else f"{self._name_str(d=d)}"

    def _equal(self, k: str, v: typing.Union[str, int, None], symbol: str = 'eq', reverse: bool = False) -> str:
        """
        判断相等
        :param k: 键
        :param v: 值
        :param symbol: 符号 eq is like in
        :param reverse: 反转
        :return:
        """
        _symbol = {
            True: {
                'eq': '<>',
                'is': 'NOT IS',
                'like': 'NOT LIKE',
                'in': 'NOT IN'
            },
            False: {
                'eq': '=',
                'is': 'IS',
                'like': 'LIKE',
                'in': 'IN'
            },
        }
        return f"`{k}` {_symbol[reverse][symbol]} {self._str(v)}"

    @staticmethod
    def _str(data: typing.Any, data_type: typing.Any = str) -> str:
        """
        数据转字符串
        :param data: 源数据
        :param data_type: 数据类型
        :return:
        """
        if data is None:
            return 'NULL'
        elif data_type in (list, tuple):
            return f"({', '.join(data)})"
        elif data_type == int:
            return str(int(data) if data else 0)
        elif data_type == float:
            return str(float(data) if data else 0.0)
        else:
            return f"'{data}'" if data else ''

    @staticmethod
    def _and(c: typing.Collection) -> str:
        """
        且
        :param c: 条件集合
        :return:
        """
        return ' AND '.join([f"({i})" if 'OR' in i else i for i in c])

    @staticmethod
    def _or(c: typing.Collection) -> str:
        """
        或
        :param c: 条件集合
        :return:
        """
        return ' OR '.join(c)

    @staticmethod
    def _name_str(d: str) -> str:
        """
        返回表名字符串
        :param d: 数据源
        :return: 
        """
        return f"{d}" if '.' in d else f"`{d}`"

    @staticmethod
    def item_data_2_str(data: typing.Any, value_type: typing.Any) -> str:
        """
        根据数据类型处理数据字符串
        :param data: 源数据
        :param value_type: 要转换的类型
        :return:
        """
        _data = data  # 数据备份
        # if re.match(r'int', value_type):  # 整型
        if value_type == int:  # 整型
            ret_str = f"{int(_data) if _data or _data == 0 else 0}"
        elif float == value_type:  # 浮点型
            ret_str = f"{float(_data) if _data or _data == 0 else 0.0}"
        else:  # 字符串
            if isinstance(_data, float):
                _data = int(_data)  # 浮点型转整型
            if not _data:
                _data = ''  # 空类型转空字符串
            ret_str = f"\"{_data}\""
        return re.sub(r'None', r'null', ret_str)  # 返回

    def _cache_execute(self, sql: str, cache_sql: str = '', exe_type: str = 'select',
                       print_sql: bool = False) -> typing.Any:
        """
        缓存装饰器
        :param sql: 要执行的查询语句
        :param cache_sql: 缓存的查询语句
        :param exe_type: 执行类型 insert delete update select
        :param print_sql: 是否打印语句
        :return:
        """
        _cache_sql = cache_sql if cache_sql else sql
        # 判断是否存在查询缓存
        if _cache_sql in self.cache_dict:
            # print(f"使用缓存")
            _results = self.cache_dict[_cache_sql]
        else:
            # print(f"追加缓存")
            # 打印sql语句
            _results = self._execute(sql=sql, exe_type=exe_type, print_sql=print_sql)
            # 追加缓存
            self.cache_dict[_cache_sql] = _results
        return _results

    # @time_statistics
    def _execute(self, sql: str, exe_type: str = 'select', print_sql: bool = False) -> typing.Any:
        """
        执行sql语句
        :return:
        """
        # 打印sql语句
        if print_sql:
            self._print_sql(sql=sql)
        try:
            # 执行查询语句
            _results = self.cursor.execute(query=sql)
            # 特定数量数据提交
            if exe_type in ['insert', 'delete', 'update']:
                # 判断条数是否大于0
                if _results > 0:
                    self.count_dict[exe_type] += _results
                # 数量达道提交数量
                if self.count_dict[exe_type] % self.commit_num == 0:
                    self.conn.commit()
                    print(f"数据提交，提交数量：{self.commit_num}")
        except Exception as e:
            self.conn.rollback()
            print(f"异常回滚数据")
            raise e
        # 返回数据
        if exe_type in ['select']:
            return self.cursor.fetchall()  # 获取执行结果
        elif exe_type in ['insert']:
            return self.cursor.lastrowid  # 新插入数据的id
        else:
            return _results

    @staticmethod
    def _print_sql(sql: str) -> None:
        """
        统一sql打印
        :param sql:
        :return:
        """
        print(f"{sql}")

    @staticmethod
    def _get_start_step(select_dict: typing.Dict) -> typing.Tuple[int, int]:
        """
        获取开始行和步长
        :param select_dict: 查询字典
        :return:
        """
        _step = select_dict.get('step', 1000)
        _start = select_dict.get('start', 0)
        return _start, _step

    @staticmethod
    def _data_dict_cleaning(data_dict: typing.Dict) -> typing.Dict:
        """
        清洗数据
        :param data_dict: 要清洗数据
        :return:
        """
        # 数据备份
        ret_data_dict = copy.deepcopy(data_dict)
        # 遍历数据
        for _key, _value in data_dict.items():
            # 判断是否有效数据
            if _value is None:
                ret_data_dict[_key] = ''
        return ret_data_dict

    def __del__(self) -> None:
        if hasattr(self, 'cursor'):
            self.cursor.close()  # 释放游标
        if hasattr(self, 'conn'):
            self.conn.commit()  # 数据提交
            self.conn.close()  # 释放连接
            print(f"数据提交 释放连接")
