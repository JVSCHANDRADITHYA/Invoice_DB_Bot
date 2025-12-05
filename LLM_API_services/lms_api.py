import requests

class LMstudioAPI:
    def __init__(self, base_url='http://localhost:', portno=None, model_name='google/gemma-3-1b'):
        
        if portno:
            self.base_url = f'{base_url}{portno}'
        else:
            self.base_url = f'{base_url}1234'

        # check if LMS API is reachable
        try:
            response = requests.get(f'{self.base_url}/v1/models')
            response.raise_for_status()
            print(f"LMS API is reachable at {self.base_url}")
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to LMS API at {self.base_url}: {e}")
            raise

        self.model_name = model_name
        

    def list_models(self):
        response = requests.get(f'{self.base_url}/v1/models')
        response.raise_for_status()
        return response.json()

    def get_model_info(self, model_name):
        response = requests.get(f'{self.base_url}/v1/models/{model_name}')
        response.raise_for_status()
        return response.json()
    
    def get_response(self, user_prompt, system_prompt = None, model_name=None, max_tokens=512, temperature=0.7):
        if model_name is None:
            model_name = self.model_name
        
        if system_prompt is None:
            system_prompt = "You are a an SQL database expert. Answer the user's questions based on the provided database schema and data."

        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": temperature
        }

        response = requests.post(f'{self.base_url}/v1/chat/completions', json=payload)
        response.raise_for_status()
        return response.json()
    
if __name__ == '__main__':
    api = LMstudioAPI(portno=1234)
    models = api.list_models()
    print("Available models:", models)

    model_info = api.get_model_info('google/gemma-3-1b')
    print("Model info:", model_info)

    user_prompt = "What is the capital of France?"
    response = api.get_response(user_prompt=user_prompt)
    print("Response:", response)