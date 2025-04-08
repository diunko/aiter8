import os
import json
from typing import Any, Dict, Optional, Union
from openai import OpenAI

# Initialize the OpenAI client with API key from environment
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def llm_json(
    prompt: str,
    model: str = "gpt-4o",
    temperature: float = 0.1,
    max_tokens: int = 1000,
    response_format: Dict[str, str] = {"type": "json_object"},
) -> Dict[str, Any]:
    """
    Calls OpenAI's GPT-4o model to generate a structured JSON response.
    
    Args:
        prompt: The prompt to send to the model. Should specify the desired JSON format.
        model: The OpenAI model to use. Defaults to "gpt-4o".
        temperature: Controls randomness. Lower values are more deterministic.
        max_tokens: Maximum number of tokens in the response.
        response_format: Format specification for the response.
    
    Returns:
        The parsed JSON response as a Python dictionary.
    
    Example:
        result = llm_json(
            "Count the words in this sentence and return as JSON",
            response_format={"type": "json_object"}
        )
        # Returns something like: {"word_count": 9}
    """
    try:
        # Make the API call to OpenAI
        response = client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that always returns valid JSON."},
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract and parse the JSON response
        json_str = response.choices[0].message.content
        return json.loads(json_str)
    
    except Exception as e:
        print(f"Error in llm_json: {e}")
        # Return an error response as JSON
        return {"error": str(e), "success": False}


def llm_text(
    prompt: str,
    model: str = "gpt-4o",
    temperature: float = 0.5,
    max_tokens: int = 1000,
) -> str:
    """
    Calls OpenAI's GPT-4o model to generate a text response.
    
    Args:
        prompt: The prompt to send to the model.
        model: The OpenAI model to use. Defaults to "gpt-4o".
        temperature: Controls randomness. Higher values make response more creative.
        max_tokens: Maximum number of tokens in the response.
    
    Returns:
        The generated text response.
    """
    try:
        # Make the API call to OpenAI
        response = client.chat.completions.create(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract the text response
        return response.choices[0].message.content
    
    except Exception as e:
        print(f"Error in llm_respond: {e}")
        return f"Error generating response: {e}" 