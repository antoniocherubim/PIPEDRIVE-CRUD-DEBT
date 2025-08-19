#!/usr/bin/env python3
"""
Script de entrada para executar os utilitários do sistema

Este script facilita a execução dos utilitários sem navegar pelas pastas.
"""

import sys
import os

# Adicionar pasta utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

if __name__ == "__main__":
    from utils.menu_principal import main as menu_main
    menu_main() 