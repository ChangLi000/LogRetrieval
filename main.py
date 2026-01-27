import agent
from rag import Analyzer
from crewai import Crew, Process
from crewai.flow.flow import Flow, listen, start, router
from agent import QueryRewriterAgent, DataRetrievalEngineerAgent, DataRetrievalExecutorAgent, DataRetrievalManager
from task import QueryRewriteTask, DataRetrievalTask
from model import CustomLLM
import os

from pydantic import BaseModel, Field


TEST_QUERY_1 = "张三2025年10月1日登录了几次邮件？"
TEST_QUERY_2 = "IP是203.96.238.136在哪些账号上使用过？"
TEST_QUERY_3 = "IP是203.96.238.136的子网IP在哪些账号上使用过？"

api_key = os.environ.get("OPENAI_API_KEY", "")
model_name = os.environ.get("OPENAI_MODEL_NAME", "")
endpoint = os.environ.get("OPENAI_ENDPOINT", "")
llm = CustomLLM(api_key=api_key, model=model_name, endpoint=endpoint, temperature=0.0, top_p=1.0)  

class MainFlowState(BaseModel):
    userInput: str = Field("", description="The user input for the flow")


class MainFlow(Flow[MainFlowState]):
    def __init__(self):
        super().__init__(tracing=True)

    @start()
    def QueryRewrite(self):
        analyzer = Analyzer()
        self.extra_information = analyzer.analyze(self.state.userInput)
        agent = QueryRewriterAgent(llm=llm)
        rewrite_task = QueryRewriteTask(
            user_question=self.state.userInput,
            extra_information=self.extra_information,
            agent=agent  # Writer leads, but can delegate research to researcher
        )
        crew = Crew(
            agents=[agent],
            tasks=[rewrite_task],
            process=Process.sequential,
            verbose=True
        )
        result = crew.kickoff()
        return result
    
    @listen("QueryRewrite")
    def DataRetrieval(self, RewriteQuery):
        # Engineer = DataRetrievalEngineerAgent(llm=llm)
        Executor = DataRetrievalExecutorAgent(llm=llm)
        # Manager = DataRetrievalManager(llm=llm)

        retrieval_task = DataRetrievalTask(
            user_question=RewriteQuery,
            extra_information=self.extra_information,
            agent=Executor  # Writer leads, but can delegate research to researcher
        )
        crew = Crew(
            agents=[Executor],
            tasks=[retrieval_task],
            process=Process.sequential,  # Manager coordinates everything
            # manager_llm=llm,  # Specify LLM for manager
            verbose=True
        )
        result = crew.kickoff()
        return result



def main():
    state = {
        "userInput": "提取10.100.31.106在过去一天遭受的攻击的日志"
    }
    flow = MainFlow()
    result = flow.kickoff(state)
    print(result)

if __name__ == "__main__":
    main()
