from fastapi import FastAPI
from pydantic import BaseModel
from llama_cpp import Llama

app = FastAPI()

# Load llama.cpp CPU model (adjust path)
llm = Llama(
    model_path="models/gemma.gguf",
    n_threads=6,          # adjust for your CPU
    n_ctx=4096,
)

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 2000
    temperature: float = 0.15

@app.post("/generate")
def generate_text(req: GenerateRequest):
    output = llm(
        req.prompt,
        max_tokens=req.max_tokens,
        temperature=req.temperature,
        stop=["</s>", "SQL ONLY:"],
    )

    text = output["choices"][0]["text"]
    return {"text": text}
