"""
Configurações de Rate Limiting para API do Pipedrive
Permite ajustar facilmente os parâmetros de controle de requisições
"""

# Configurações de Rate Limiting
RATE_LIMIT_CONFIG = {
    # Limite de requisições por minuto (recomendado: 60-80 para ser conservador)
    'requests_per_minute': 60,
    
    # Delay adicional entre requisições (em segundos)
    'delay_between_requests': 0.5,
    
    # Configurações de retry para erro 429
    'max_retries': 3,
    'base_delay': 1,
    'max_delay': 300,  # 5 minutos máximo
    
    # Delays específicos para erro 429 (em segundos)
    '429_delays': [60, 120, 180],  # 1min, 2min, 3min
    
    # Tempo de "resfriamento" após erro 429 (em segundos)
    '429_cooldown_time': 120,  # 2 minutos
    
    # Configurações de processamento
    'max_concurrent_requests': 2,  # Threads simultâneas
    'batch_size': 25,  # Itens por lote
}

# Configurações conservadoras (para quando há muitos rate limits)
CONSERVATIVE_CONFIG = {
    'requests_per_minute': 40,
    'delay_between_requests': 1.0,
    'max_retries': 5,
    'base_delay': 2,
    'max_delay': 600,  # 10 minutos máximo
    '429_delays': [120, 240, 360, 480, 600],  # 2min, 4min, 6min, 8min, 10min
    '429_cooldown_time': 300,  # 5 minutos
    'max_concurrent_requests': 1,  # Apenas 1 thread
    'batch_size': 10,  # Lotes menores
}

# Configurações agressivas (para quando a API está estável)
AGGRESSIVE_CONFIG = {
    'requests_per_minute': 80,
    'delay_between_requests': 0.2,
    'max_retries': 2,
    'base_delay': 1,
    'max_delay': 60,  # 1 minuto máximo
    '429_delays': [30, 60],  # 30s, 1min
    '429_cooldown_time': 60,  # 1 minuto
    'max_concurrent_requests': 3,  # 3 threads
    'batch_size': 50,  # Lotes maiores
}

def get_rate_limit_config(config_type='default'):
    """
    Retorna configuração de rate limiting
    
    Args:
        config_type: 'default', 'conservative', ou 'aggressive'
    
    Returns:
        Dict com configurações
    """
    if config_type == 'conservative':
        return CONSERVATIVE_CONFIG
    elif config_type == 'aggressive':
        return AGGRESSIVE_CONFIG
    else:
        return RATE_LIMIT_CONFIG

def get_optimal_config_based_on_errors(error_count_last_hour=0):
    """
    Retorna configuração otimizada baseada no número de erros recentes
    
    Args:
        error_count_last_hour: Número de erros 429 na última hora
    
    Returns:
        Dict com configurações otimizadas
    """
    if error_count_last_hour > 10:
        return get_rate_limit_config('conservative')
    elif error_count_last_hour > 5:
        return get_rate_limit_config('default')
    else:
        return get_rate_limit_config('aggressive')
