# tap_ai/infra/llm_client.py (New file)  
  
"""  
Shared LLM client singleton for TAP AI services  
"""  
  
from typing import Optional  
from langchain_openai import ChatOpenAI  
from tap_ai.infra.config import get_config  
  
class LLMClient:  
    """Singleton LLM client manager"""  
      
    _instances = {}  
      
    @classmethod  
    def get_client(cls, model: str = "gpt-4o-mini", temperature: float = 0.0,   
                   max_tokens: int = 1500) -> ChatOpenAI:  
        """  
        Get or create a cached LLM client instance.  
          
        Args:  
            model: Model name  
            temperature: Temperature setting  
            max_tokens: Max tokens setting  
              
        Returns:  
            ChatOpenAI instance  
        """  
        cache_key = f"{model}_{temperature}_{max_tokens}"  
          
        if cache_key not in cls._instances:  
            api_key = get_config("openai_api_key")  
            if not api_key:  
                raise ValueError("OpenAI API key not configured")  
                  
            cls._instances[cache_key] = ChatOpenAI(  
                model_name=model,  
                openai_api_key=api_key,  
                temperature=temperature,  
                max_tokens=max_tokens,  
            )  
          
        return cls._instances[cache_key]  
      
    @classmethod  
    def clear_cache(cls):  
        """Clear all cached instances"""  
        cls._instances.clear()  
  

 