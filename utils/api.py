# Pacote python com funções de chamada de API que serão reutilizadas em varios notebooks

# imports

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# Função executa api paginada

# Referencia retry + backof => https://medium.com/@bounouh.fedi/enhancing-resilience-in-python-applications-with-tenacity-a-comprehensive-guide-d92fe0e07d8

# Função que chama a API com retry + backoff 
# Em casos de exceções da biblioteca requests ocorrerar outras tentativas com intervalos de 2s, 4s, 8s, 16s e 30s
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    reraise=True
)
def buscar_dados_paginado(base_url, params={}, itensporPagina=100):

    resultado = []
    pagina = 1

    while True:
        
        # Seta Numero da Pagina  e Numero de itens por Pagina para cada Requisição
        params['pagina'] = pagina 
        params['itens'] = itensporPagina

        #Executa Requisição
        resposta = requests.get(base_url, params=params, timeout=30)  

        #Lança exceção para códigos de erro HTTP
        resposta.raise_for_status()
    
        saida = resposta.json()
            
        # Adiciona resultado da pagina a lista de resultados
        resultado.extend(saida['dados'])

        #verifica se há mais paginas
        if len(saida['dados']) < itensporPagina:
            break
            
        # incrementa Numero da pagina
        pagina+=1

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            break

    return  resultado


    
#Função para bsucar dados de endpoints sem paginaçao
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    reraise=True
)
def buscar_dados(base_url, params={}):
    
    #Executa Requisição
    resposta = requests.get(base_url, params=params, timeout=30)  

    #Lança exceção para códigos de erro HTTP
    resposta.raise_for_status()
    
    resultado = resposta.json()

    return  resultado

    