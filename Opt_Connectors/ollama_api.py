import requests

class OllamaAPI:
    def __init__(self, base_url='http://localhost:11434'):
        self.base_url = base_url

    def list_models(self):
        response = requests.get(f'{self.base_url}/models')
        response.raise_for_status()
        return response.json()

    def get_model_info(self, model_name):
        response = requests.get(f'{self.base_url}/models/{model_name}')
        response.raise_for_status()
        return response.json()

    def generate_text(self, model_name, prompt, max_tokens=100):
        payload = {
            'model': model_name,
            'prompt': prompt,
            'max_tokens': max_tokens
        }
        response = requests.post(f'{self.base_url}/generate', json=payload)
        response.raise_for_status()
        return response.json()
    def stream_generate_text(self, model_name, prompt, max_tokens=100):
        payload = {
            'model': model_name,
            'prompt': prompt,
            'max_tokens': max_tokens,
            'stream': True
        }
        response = requests.post(f'{self.base_url}/generate', json=payload, stream=True)
        response.raise_for_status()
        for line in response.iter_lines():
            if line:
                yield line.decode('utf-8')
if __name__ == '__main__':
    api = OllamaAPI()

    # List available models
    models = api.list_models()
    print("Available Models:", models)

    # Get information about a specific model
    model_info = api.get_model_info('llama3.2')
    print("Model Info:", model_info)

    # Generate text using a model
    prompt = "Once upon a time"
    generated_text = api.generate_text('llama3.2', prompt)
    print("Generated Text:", generated_text)

    # Stream text generation
    print("Streaming Generated Text:")
    for chunk in api.stream_generate_text('llama3.2', prompt):
        print(chunk)
