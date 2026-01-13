from openai import OpenAI
import os

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

response = client.responses.create(
    model="gpt-4.1",
    input="Explain why missing IV history blocks trade execution. Use no predictions.",
)

print(response.output_text)