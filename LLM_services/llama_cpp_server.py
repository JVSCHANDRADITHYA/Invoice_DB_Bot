from fastapi import FastAPI
from pydantic import BaseModel
from llama_cpp import Llama

app = FastAPI()

# Load the model once (persistent)
llm = Llama(
    model_path=r"C:\Users\adith\.lmstudio\models\lmstudio-community\gemma-3-1B-it-qat-GGUF\gemma-3-1B-it-QAT-Q4_0.gguf",
    n_ctx=4096,
    n_threads=8
)

class Prompt(BaseModel):
    prompt: str
    max_tokens: int = 256

@app.post("/generate")
def generate_text(req: Prompt):
    output = llm(
        req.prompt,
        max_tokens=req.max_tokens
    )
    return {"output": output["choices"][0]["text"]}
