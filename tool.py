from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from typing import Type, Optional
from datetime import timedelta, datetime
import os
from elasticsearch import Elasticsearch

elasticsearch_usr = os.environ.get("ELK_USR", "")
elasticsearch_pwd = os.environ.get("ELK_PWD", "")


class LogRetrievalToolInput(BaseModel):
    """Input schema for MyCustomTool."""
    Ip: str = Field(..., description="目标IP地址")
    Index: str = Field(..., description="ELK索引名称")
    StartTime: Optional[str] = Field(None, description="查询开始时间，格式为YYYY-MM-DD HH:MM:SS，默认为过去24小时")
    EndTime: Optional[str] = Field(None, description="查询结束时间，格式为YYYY-MM-DD HH:MM:SS，默认为当前时间")
    
    
class LogRetrievalBasedOnIp(BaseTool):
    name: str = "LogRetrievalBasedOnIp"
    description: str = "基于输入告警的目标IP进行查询，然后把查询到日志的内容返回"
    args_schema: Type[BaseModel] = LogRetrievalToolInput

    def _format_to_markdown(self, data_list):
        """将字典列表格式化为Markdown表格"""

        # 获取表头
        d1 = data_list[0]
        keys = d1.keys()
        headers = list(keys)

        # 构建表头行
        markdown = "| " + " | ".join(headers) + " |\n"

        # 构建分隔行
        markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"

        # 构建数据行
        for item in data_list:
            row = []
            for header in headers:
                value = item[header]
                row.append(str(value))
            markdown += "| " + " | ".join(row) + " |\n"

        return markdown

    def _run(self, Ip: str, Index: str, StartTime: Optional[str] = None, EndTime: Optional[str] = None) -> str:
        url = "http://159.226.16.247:9200/"
        print("Using Elasticsearch username:", elasticsearch_usr)
        print("Using Elasticsearch password:", elasticsearch_pwd)
        # 注意：这里需要通过其他方式获取默认时间，因为无法直接在类上调用实例方法
        if StartTime is None:
            now = datetime.now()
            start_time = now - timedelta(hours=1)
            StartTime = int(start_time.timestamp())  # 转为整数时间戳（秒）
        else:
            StartTime = int(datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S").timestamp())

        if EndTime is None:
            EndTime = int(datetime.now().timestamp())
        else:
            EndTime = int(datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S").timestamp())

        print("Using start time:", StartTime, "Using end time:", EndTime)
        query = {
            "query": {
                "bool": {
                    "must": [
                                {
                                   "term": {
                                        "IP": Ip
                                    }
                                },
                                {
                                    "range": {
                                        "create_date": {
                                                "gte": str(StartTime),
                                                "lte": str(EndTime)
                                        }
                                    }
                                } 
                    ]
                }
            },
            "from": 0
        }
        es = Elasticsearch([url],basic_auth=(elasticsearch_usr,elasticsearch_pwd))
        print("Using query:", query)
        response = es.search(index=Index, body=query, size=10)
        # 提取命中的数据
        hits = response['hits']['hits']
        if hits:
            # 提取源数据并转换为列表格式
            data_list = [hit['_source'] for hit in hits]
            # 将结果格式化为markdown表格
            print("Using markdown_result:", data_list)
            markdown_result = self._format_to_markdown(data_list)
            return markdown_result
        else:
            return f"在索引 {Index} 中未找到匹配 IP {Ip} 的日志数据"


def main():
    # 示例：使用新工具类
    tool = LogRetrievalBasedOnIp()
    # 如果提供时间参数，则使用提供的参数；否则将使用默认的过去24小时
    result = tool._run(Ip="10.100.31.106", Index="pass_user_action_2026*")
    print(result)
if __name__ == "__main__":
    main()
