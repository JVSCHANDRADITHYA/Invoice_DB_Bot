from llama_cpp import Llama

llm = Llama(
    model_path=r"C:\Users\adith\.lmstudio\models\lmstudio-community\gemma-3-1B-it-qat-GGUF\gemma-3-1B-it-QAT-Q4_0.gguf",
    n_ctx=4096,
    n_threads=8
)

output = llm("Explain transformers in simple terms.", max_tokens=256)
print("Here's the answer")
print(output["choices"][0]["text"])
