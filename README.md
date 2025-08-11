# Pipeline de atualização de dados – Projeto Arboili
Este repositório contém scripts para extração, processamento e atualização periódica dos dados utilizados no painel interativo do **Projeto Arboili**. Ele permite integrar e organizar informações epidemiológicas e de interesse de busca, garantindo que o dashboard esteja sempre atualizado para apoiar a análise e o monitoramento de doenças.

## Funcionalidades principais
- Extração de notificações de casos do SINAN e SIVEP.
- Coleta de dados de interesse de busca no Google Trends.
- Processamento e padronização dos dados em formato pronto para visualização.
- Atualização automática dos arquivos utilizados no dashboard.
  
## Fontes de dados
**Notificações**:
  - Casos de Dengue e Chikungunya – SINAN
  - Síndromes Respiratórias Agudas Graves (SRAGs) – SIVEP
    
**Interesse de busca**:
  - Google Trends – [https://trends.google.com/trends/]

## Reprodutibilidade
Para reproduzir o processo de atualização, é necessário obter credenciais de acesso à API utilizada para extração dos dados do **Google Trends**.
As credenciais devem ser informadas no local indicado dentro da função `update_pipeline.R`.

## Financiamento
Inova Fiocruz
