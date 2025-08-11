import requests

class TelegramNotifier:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    def notify(self, message, silent=False):
        """
        Envia uma mensagem para o chat configurado
        
        Args:
            message (str): Mensagem a ser enviada
            silent (bool): Se True, envia a notificação silenciosamente
        """
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "MarkdownV2",
                "disable_notification": False,
                "disable_web_page_preview": True,
                "protect_content": False,
                "allow_sending_without_reply": True,
                "notification": True 
            }
            
            # Adiciona cabeçalho para prioridade alta
            headers = {
                "X-Telegram-Priority": "high"
            }
            
            # Escapa caracteres especiais do Markdown
            special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
            for char in special_chars:
                message = message.replace(char, f'\\{char}')
            
            data["text"] = message
            
            # Tenta enviar a mensagem com prioridade alta
            response = requests.post(url, json=data, headers=headers)
            
            if not response.ok:
                # Se falhar, tenta enviar como mensagem normal
                response = requests.post(url, json=data)
                if not response.ok:
                    print(f"Erro ao enviar mensagem: {response.text}")
                    
        except Exception as e:
            print(f"Erro ao enviar mensagem para o Telegram: {e}")
            
    def send_alert(self, title, message):
        """
        Envia uma mensagem de alerta com formatação especial
        """
        formatted_message = f"🔔 *ALERTA*\n\n*{title}*\n{message}"
        self.notify(formatted_message, silent=False)