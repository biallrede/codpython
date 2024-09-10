import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy
import numpy as np
from scipy import stats
from prophet import Prophet
import psycopg2
import openpyxl
from sqlalchemy import create_engine
import schedule
import threading
import cmdstanpy


def previsaoativacao():
    # Conectar ao banco de dados
    engine = sqlalchemy.create_engine('postgresql+psycopg2://erick_leitura:73f4cc9b2667d6c44d20d1a0d612b26c5e1763c2@134.65.24.116:9432/hubsoft')

    # Definir a consulta SQL
    query = """
    SELECT 
        A.data_habilitacao::date AS data_habilitacao, 
        COUNT(A.id_cliente_servico) AS quantidade_clientes_ativados
    FROM 
        cliente_servico A
    WHERE 
        A.origem = 'novo' 
        AND A.id_servico NOT IN (5165, 8134, 4179, 4219, 9451)  -- Aluguel de Porta, Locação de Infraestrutura, Rede Neutra
        AND A.data_habilitacao IS NOT NULL
        AND A.data_habilitacao::date >= '2024-02-01'
        AND A.data_habilitacao::date <= CURRENT_DATE
    GROUP BY 
        A.data_habilitacao::date
    ORDER BY 
        A.data_habilitacao::date;
    """
    #importacao e tratamento de dados
    habilitacao_ = pd.read_sql_query(query, engine)
    habilitacao_['data_habilitacao']=pd.to_datetime(habilitacao_['data_habilitacao'])
    dataset=habilitacao_.iloc[:, :2]
    dataset = dataset[['data_habilitacao', 'quantidade_clientes_ativados']].rename(columns = {'data_habilitacao': 'ds', 'quantidade_clientes_ativados': 'y'})

    #previsao
    model = Prophet(seasonality_mode='multiplicative')
    model.fit(dataset)
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    habilitacao_.set_index('data_habilitacao', inplace=True)
    indice = pd.date_range('2024-01-01', periods = len(habilitacao_), freq = 'd')
    serie = pd.Series(habilitacao_['quantidade_clientes_ativados'].values,index=indice)
    serie_atual = serie

    # outliers
    z_scores = np.abs(stats.zscore(serie_atual))
    limite = 3
    serie = serie_atual[(z_scores < limite)]
    #medimovel
    media_movel = serie.rolling(window=7)
    media_movel = media_movel.mean()
    media_movel=media_movel.dropna()
    media_movel = media_movel.dropna()
    ultima_media_movel = media_movel.iloc[-1]
    previsaoMediaMovel = pd.DataFrame(index=pd.date_range(start=serie.index[-1] + pd.Timedelta(days=1), periods=30))
    previsaoMediaMovel['media_movel'] =round(ultima_media_movel)
    previsaoMediaMovel = previsaoMediaMovel.reset_index(drop=True)

    #previsaoTabela
    forecast_tail = round(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(30))
    forecast_tail['yhat_lower'] = forecast_tail['yhat_lower'].apply(lambda x: max(x, 0))
    resultado = pd.concat([forecast_tail.reset_index(drop=True), previsaoMediaMovel.reset_index(drop=True)], axis=1)

    # Credenciais de acesso
    usuario = 'user_allnexus'
    senha = 'uKl041xn8HIw0WF'
    servidor = '187.121.151.19'
    banco_de_dados = 'DB_ATENDIMENTO'
    tabela = 'PREVISAO_ATIVACOES'

    # Criando a conexão com o SQL Server
    engine = create_engine(f'mssql+pyodbc://{usuario}:{senha}@{servidor}/{banco_de_dados}?driver=ODBC+Driver+17+for+SQL+Server')

    # Enviar os resultados para o banco de dados
    resultado.to_sql(tabela, engine, if_exists='append', index=False)


def agendar_mensalmente(dia_do_mes, hora):
    """Função que agenda uma tarefa mensalmente em um dia e hora específicos."""
    def tarefa():
        agora = datetime.now()
        if agora.day == dia_do_mes and agora.strftime("%H:%M") == hora:
            previsaoativacao()

    schedule.every().minute.do(tarefa)

# Defina o dia do mês e a hora para executar a rotina
DIA_DO_MES = 1  # Exemplo: dia 1 de cada mês
HORA = "02:00"  # Hora para execução

# Agendamento
agendar_mensalmente(DIA_DO_MES, HORA)

def iniciar_agendador():
    while True:
        schedule.run_pending()
        threading.Event().wait(1)  # Aguarda 1 segundo entre verificações

# Executa o agendador em uma nova thread
scheduler_thread = threading.Thread(target=iniciar_agendador)
scheduler_thread.start()