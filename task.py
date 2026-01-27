from crewai import Task

class QueryRewriteTask(Task):
    def __init__(self, *args, user_question="", extra_information="", agent=None, **kwargs):
        # 先格式化描述字符串
        description = """You are given a user question and additional contextual information.
            Your task is to rewrite and integrate them into a single, structured query specification
            that can be directly consumed by a downstream Text-to-SQL agent.
            Input:
            1. User Question:
            {user_question}
            2. Extra Information:
            {extra_information}
            Instructions:
            - Integrate the user question and the extra information into ONE coherent query specification.
            - Make all implicit constraints explicit when supported by the inputs.
            - Use database-aligned terminology (tables, columns, conditions).
            - Resolve ambiguity conservatively; do not invent missing information.
            - Do NOT generate SQL.
            - Do NOT include execution steps or reasoning traces.
            Output:
            Follow EXACTLY the structured format required by the agent.""".format(
                user_question=user_question, 
                extra_information=extra_information
            )
        
        kwargs.setdefault("description", description)
        kwargs.setdefault("expected_output", "A structured query specification")
        kwargs.setdefault("agent", agent)
        super().__init__(*args, **kwargs)



class DataRetrievalTask(Task):
    def __init__(self, *args, user_question="", extra_information="", agent=None, **kwargs):
        # 先格式化描述字符串
        description = """Execute the retrieval using appropriate log retrieval tools to retrieve the requested log data.
            Input:
            1. User Question:
            {user_question}
            2. Extra Information:
            {extra_information}.""".format(
                user_question=user_question, 
                extra_information=extra_information
            )
        
        kwargs.setdefault("description", description)
        kwargs.setdefault("expected_output", "raw log data")
        kwargs.setdefault("agent", agent)
        super().__init__(*args, **kwargs)