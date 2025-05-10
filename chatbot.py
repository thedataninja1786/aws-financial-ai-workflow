import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

def generate_sentiment(symbol:str, date:str) -> str:
    api_key = os.getenv("OPENAI_KEY").strip()
    response = ""
    prompt = f" Act as a financial analyst, in two sentences what is the sentiment for {symbol} on {date}?"

    try:
        client = OpenAI(api_key=api_key)
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="gpt-4o-mini",
        )
        if chat_completion.choices:
            response = chat_completion.choices[0].message.content
    except Exception as e:
        print(
            f"{generate_sentiment.__name__} - the following exception has occurred: {e}"
        )
        raise
    finally:
        return response