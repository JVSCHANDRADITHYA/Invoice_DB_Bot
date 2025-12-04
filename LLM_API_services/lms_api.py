import requests

class LMstudioAPI:
    def __init__(self, base_url='http://localhost:1234'):
        self.base_url = base_url

    def list_models(self):
        response = requests.get(f'{self.base_url}/api/models')
        response.raise_for_status()
        return response.json()

    def get_model_info(self, model_name):
        response = requests.get(f'{self.base_url}/api/models/{model_name}')
        response.raise_for_status()
        return response.json()

    def generate_text(self, model_name, prompt, max_tokens=100):
        payload = {
            'model': model_name,
            'prompt': prompt,
            'max_tokens': max_tokens
        }
        response = requests.post(f'{self.base_url}/api/generate', json=payload)
        response.raise_for_status()
        return response.json()

    def stream_generate_text(self, model_name, prompt, max_tokens=100):
        payload = {
            'model': model_name,
            'prompt': prompt,
            'max_tokens': max_tokens,
            'stream': True
        }
        response = requests.post(f'{self.base_url}/api/generate', json=payload, stream=True)
        response.raise_for_status()
        for line in response.iter_lines():
            if line:
                yield line.decode('utf-8')