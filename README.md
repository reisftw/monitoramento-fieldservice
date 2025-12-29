# 🛰️ Monitor Unificado de serviços em python
> **Versão 2.4** | Inteligência Operacional & Monitoramento de Rede em Tempo Real.

O **Monitor Unificado** é uma solução robusta desenvolvida em Python para a **Sempre Internet**, focada na automação do monitoramento de infraestrutura e otimização de serviços operacionais. A ferramenta integra APIs de geolocalização, dashboards de BI e sistemas de mensageria para reduzir o tempo de resposta a incidentes e garantir a continuidade dos serviços.

---

## 🚀 Impacto Operacional
O uso desta ferramenta na operação visa a excelência técnica e agilidade na tomada de decisão:

* **MTTR Reduzido**: Identificação instantânea de quedas massivas por bairro e cidade.
* **Qualidade Técnica**: Monitoramento rigoroso de retrabalhos para identificar falhas recorrentes em processos, técnicos ou empresas parceiras.
* **Gestão de SLA**: Alertas proativos de Ordens de Serviço (O.S.) com prazo crítico ou já vencido.
* **Planejamento Preventivo**: Integração com previsão do tempo para gestão de equipes de campo em dias de chuva.

---

## 🛠️ Tecnologias e Stack

| Tecnologia | Aplicação |
| :--- | :--- |
| **Python 3.10+** | Núcleo do processamento e lógica do sistema |
| **SQLite3** | Banco de dados local para persistência de histórico e BI |
| **curl_cffi** | Requisições de API de alta performance e bypass de bloqueios |
| **Pandas** | Processamento e análise de grandes volumes de dados de rede |
| **Telegram Bot API** | Disparo automático de alertas críticos para os gestores |

---

## 📋 Funcionalidades Principais

### 1. Monitoramento de Rede (Real-time)
O sistema realiza varreduras cíclicas para detectar:
* **Quedas Massivas**: Alerta quando o número de clientes offline em um bairro ultrapassa o limite configurado (padrão: 20).
* **Recuperação de Sinal**: Notificação automática quando uma região previamente afetada é normalizada.
* **Painel de Clientes**: Busca interativa por Nome, Login ou Endereço com link direto para o Google Maps.

### 2. Inteligência de BI (Business Intelligence)
Relatórios gerados a partir do banco de dados `historico_monitor.db`:
* **Ranking de Ofensores**: Identificação de técnicos e empresas com maior índice de retrabalho.
* **Morning Call**: Resumo matinal automático com status da rede, clima e pendências do dia.
* **Raio-X da Cidade**: Histórico completo de instabilidades e quedas por localidade.

---

## ⚙️ Instalação e Execução

### 1. Instalar Dependências
Abra o terminal e execute o comando abaixo:
pip install curl_cffi requests pandas playsound

### 2. Estrutura de Diretórios
O sistema cria automaticamente as seguintes pastas na primeira execução para organizar os arquivos de saída:

 * 📂 sons/: Destinada aos arquivos de áudio para alertas sonoros (sirene.mp3, alerta_os.mp3, etc).
 * 📂 info/: Armazena relatórios de BI, rankings de técnicos/empresas e Morning Calls exportados em formato TXT.
 * 📂 whatsapp/: Local onde são salvos os templates de mensagens formatadas para grupos de cobrança de SLA.
 * 📂 Quedas/: Contém os relatórios técnicos detalhados gerados para cada evento de queda massiva detectado.

## 3. Inicialização
Para iniciar o monitoramento e acessar o menu principal, utilize o comando abaixo no terminal:

python teste.py

### 🎧 Alertas Sonoros Configuráveis
Para ativar os avisos sonoros, certifique-se de que a biblioteca playsound está funcional e insira os arquivos correspondentes na pasta /sons:

* 🔊 sirene.mp3 🚨 : Emitido durante o alerta de Quedas Massivas.
* 🔊 teste.mp3 🛠️ : Emitido ao detectar novas O.S. de Retrabalho.
* 🔊 alerta_os.mp3 📢 : Emitido para novas O.S. de Agendamento (fluxo normal).
* 🔊 ratinho.mp3 🐁 : Alerta crítico para volume excessivo de O.S. (Em Massa).
* 🔊 sucesso.mp3 ✅ : Emitido durante o alerta de Normalização/Recuperação de região offline.

Desenvolvido por Rodrigo Reis |
