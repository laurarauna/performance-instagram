# performance-instagram
Código Graph API  para monitoramento de métricas Instagram

Este repositório contém um conjunto de scripts para monitorar e analisar métricas de contas do Instagram, utilizando tanto a API Graph oficial do Facebook quanto a biblioteca Instaloader para diferentes finalidades.

**O projeto é dividido em duas frentes principais:**
- Análise Detalhada via API Graph (graphAPI_page.py): Coleta métricas aprofundadas de contas de Instagram Business que você administra. Os dados são processados e armazenados em um banco de dados SQL Server para análise histórica.
- Monitoramento de Concorrentes (players_analysis.ipynb): Rastreia o crescimento de seguidores de uma lista de perfis públicos (concorrentes ou players do mercado) e salva os resultados em uma planilha Excel.
- Visualização de Dados (Monitoramento Redes Sociais.pbix): Um painel de Power BI que se conecta ao banco de dados SQL Server para oferecer uma visualização interativa dos dados coletados. O painel inclui análises de engajamento, performance de conteúdo, demografia de público e comparativos entre contas.

## Pré-requisitos
Antes de começar, você precisará de:

- Python 3.8 ou superior.
- Acesso a um servidor de banco de dados SQL Server. (adaptável caso a saída do código seja alterada para um arquivo local como Excel)
- Uma Conta de Desenvolvedor do Facebook e um Aplicativo criado para obter acesso à API Graph.
- As contas do Instagram a serem monitoradas (com graphAPI_page.py) devem ser Contas Business e estar vinculadas a uma Página do Facebook que você administra.
- Tokens de Acesso de Página do Facebook com as permissões necessárias (instagram_basic, instagram_manage_insights, pages_show_list, pages_read_engagement).

## Uso
Com o ambiente configurado, você pode executar os scripts diretamente do seu terminal.

**Para a Coleta de Métricas Detalhadas:**
Execute o script graphAPI_page.py. Ele irá iterar sobre as contas definidas no código, coletar todos os dados e salvá-los no SQL Server.
**Para o Monitoramento de Concorrentes:**
Execute o script players_analysis.ipynb. Ele irá buscar os dados de seguidores para a lista de usuários, atualizar (ou criar) a planilha Excel e salvar os resultados.

## Saída
Na pasta 'tables_sample' há um exemplo de saídas do código 'graphAPI_page.py'. Os modelos podem ser utilizados para criação das tabelas no banco de dados.

*Recomenda-se agendar a execução desses scripts (por exemplo, com Cron no Linux ou Agendador de Tarefas no Windows) para automatizar o processo de coleta. Dependdo do númeor de páginas monitoradas com graphAPI_page.py o processo completo pode demorar de 1h30 a 2h*
