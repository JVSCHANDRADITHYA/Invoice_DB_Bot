from Ollama_Handler import OllamaHandler

handler = OllamaHandler()
q = handler.generate_SQL("Does sandhya exist as a project manahger of any project?")

print("\nFINAL SQL:", q)
