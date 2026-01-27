from typing import List
import requests
from langchain.embeddings.base import Embeddings
from langchain_chroma import Chroma
import uuid

OPENAI_API_KEY = "bd4e0cd0cd0b49e4ca7ad1767baadf3a09cbab24f7aa6a9a8486cd7e3b9d9eaf"
OPENAI_ENDPOINT = "https://uni-api.cstcloud.cn/v1/embeddings"
OPENAI_MODEL_NAME = "bge-large-zh:latest"
# OPENAI_API_KEY = "988a611b0c5abe21a576d71815821c918088dc6a50af1aac4ed88a387b43e5ee"

class EmbeddingService:
    """嵌入向量服务类"""
    
    def __init__(self):
        self.api_url = OPENAI_ENDPOINT
        self.api_token = OPENAI_API_KEY  # API密钥应该赋值给api_token
        self.model_name = OPENAI_MODEL_NAME  # 模型名称应该赋值给model_name
    
    def get_embedding(self, text: str) -> List[float]:
        """获取文本嵌入向量"""
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        data = {
            "model": self.model_name,
            "input": text
        }
        response = requests.post(self.api_url, json=data, headers=headers)

        if response.status_code == 200:
            embedding = response.json().get("data", [{}])[0].get("embedding", [])
            return embedding
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return []


class CloudEmbeddings(Embeddings):
    """LangChain兼容的嵌入向量服务"""
    
    def __init__(self):
        self.embedding_service = EmbeddingService()
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [self.embedding_service.get_embedding(text) for text in texts]

    def embed_query(self, text: str) -> List[float]:
        return self.embedding_service.get_embedding(text)


class Analyzer:
    """分析器类"""
    
    def __init__(self):
        self.embedding_service = EmbeddingService()
        self.docs = [
            {"name": "arp_vpn",
             "description": "arp系统的用户行为日志",
            },
            {"name": "cas_apache_abnormal",
             "description": "网站群apache服务器异常",
            },
            {"name": "cas_vpn",
             "description": "网站群VPN日志",
            },
            {"name": "email_access",
             "description": "邮件系统apache服务器的访问日志，存储用户的访问记录",
            },
            {"name": "email_user_action",
             "description": "邮件系统用户行为日志",
            },
            {"name": "firewall_kjy_",
             "description": "科技云防火墙日志",
            },
            {"name": "pass_user_action_2026*",
             "description": "通行证用户行为日志",
            },
            {"name": "traffic_detection_2",
             "description": "自研入侵检测系统(IDS)中流量告警日志",
            },
            {"name": "security_hills_ids_",
             "description": "安全厂商山石检测系统中流量告警日志",
            },
        ]
        documents = []
        for doc in self.docs:
            embedding = self.embedding_service.get_embedding(doc['description'])
            documents.append({
                'name': doc['name'],
                'description': doc['description'],
                'embedding': embedding,
            })

        embedding_function = CloudEmbeddings()
        collection_name = f"rag-chroma-{uuid.uuid4()}"
        chroma = Chroma(
            collection_name=collection_name,
            embedding_function=embedding_function
        )

        # 主要使用description作为索引内容，name只作为辅助
        documents_texts = [
            doc["description"]
            for doc in documents
        ]
        metadatas = [
            {
                "name": doc["name"],
                "description": doc["description"]
            }
            for doc in documents
        ]
        chroma.add_texts(texts=documents_texts, metadatas=metadatas)
        self.retriever = chroma.as_retriever()
    
    def analyze(self, question: str, topk: int=3) -> dict:
        """分析问题并返回相应的索引"""
        results = self.retriever.invoke(question)
        try:
            Chroma.delete_collection(collection_name)
        except Exception:
            try:
                chroma.delete_collection()
            except Exception:
                pass

        if results:
            name = []
            results = results[:topk]
            for result in results:
                name.append(result.metadata['name'])
            return {"所需要的日志可能包含在index_name中": name,}
        else:
            return {"index_name":"未找到相关索引"}


