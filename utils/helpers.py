from datetime import datetime, timedelta

def obter_data_sincronizacao():
    return (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d')
