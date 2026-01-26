import agent
from rag import Analyzer
from crewai import Crew, Process

from agent import QueryRewriterAgent
from task import QueryRewriteTask
from model import CustomLLM
import os


TEST_QUERY_1 = "张三2025年10月1日登录了几次邮件？"
TEST_QUERY_2 = "IP是203.96.238.136在哪些账号上使用过？"
TEST_QUERY_3 = "IP是203.96.238.136的子网IP在哪些账号上使用过？"

api_key = os.environ.get("OPENAI_API_KEY", "")
model_name = os.environ.get("OPENAI_MODEL_NAME", "")
endpoint = os.environ.get("OPENAI_ENDPOINT", "")
llm = CustomLLM(api_key=api_key, model=model_name, endpoint=endpoint, temperature=0.0, top_p=1.0)  

def main():

    analyzer = Analyzer()

    result1 = analyzer.analyze(TEST_QUERY_1)
    print(result1)
    result2 = analyzer.analyze(TEST_QUERY_2)
    print(result2)
    result3 = analyzer.analyze(TEST_QUERY_3)
    print(result3)

    agent = QueryRewriterAgent(llm=llm)
    rewrite_task = QueryRewriteTask(
        user_question=TEST_QUERY_3,
        extra_information=result3,
        agent=agent  # Writer leads, but can delegate research to researcher
    )
    crew = Crew(
        agents=[agent],
        tasks=[rewrite_task],
        process=Process.sequential,
        verbose=True
    )
    result = crew.kickoff()
    print(result)


if __name__ == "__main__":
    main()
