# MPComm — Sistemas Distribuídos — Parte 1

Evolução do exemplo [MPComm do professor](https://github.com/professorfabio/MPComm) para a **Parte 1 do Trabalho Final de Sistemas Distribuídos (2026/2)**.

**Escopo implementado:** (a) cliente-servidor por RPC e (b) multicast entre peers por sockets UDP com recuperação de perdas e ordenação **FIFO por emissor**. Não implementa a aplicação da Parte 2, ordenação total da Parte 3 nem o protocolo da Parte 4.

> **Importante:** o projeto do professor chama de *multicast* o envio de uma mensagem a cada participante via `socket.sendto` (fan-out UDP). Esta versão **preserva essa estratégia**, em vez de empregar grupos multicast IP. RPC foi aplicado **apenas ao plano de controle cliente-servidor**, não à troca de mensagens entre peers.

## Integrantes do grupo

**Universidade Federal de Goiás (UFG)** — Ciência da Computacao — Sistemas Distribuidos (2026/2).

| Integrante | Matrícula |
| --- | --- |
| Islabelle Vitoria Mendonca Bastos | 202306973 |
| Felipe Sucupira de Oliveira | 202301439 |
| Sebastião Correa Fraga Neto | 202301487 |

## 1. Como executar em poucos passos (Windows)

**Requisitos:** Python 3.9+ (recomendado 3.11 ou superior), sem dependências externas. Na pasta raiz do projeto, tanto no Windows quanto no Linux/macOS:

```powershell
python --version
python -m unittest discover -s tests -v
python demo_local.py
```

A demonstração em Python abre os servidores e peers como processos independentes, **escolhe automaticamente portas locais livres** e encerra todos ao concluir. Use `python demo_local.py --peers 6 --messages 25` para uma rodada mais ampla. Também fornecemos `demo_local.ps1` como alternativa em PowerShell com portas fixas.

Se o Windows bloquear a execução do `.ps1`, execute **somente nesta sessão**:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\demo_local.ps1
```

O script inicia automaticamente **1 gerenciador + 3 peers + 1 coordenador**, envia **8 mensagens de cada peer**, simula perdas na primeira recepção de determinados pacotes e confirma retransmissões, completude e FIFO. Os relatórios completos são gravados em `logs/demo_.../`. O script encerra os processos ao final.

### Iniciar manualmente em 5 terminais

Na raiz do projeto, abra cinco janelas de terminal (deixe todas abertas até o fim da rodada):

**Terminal 1 — gerenciador:**

```powershell
python GroupMngr.py --bind 127.0.0.1 --port 5680
```

**Terminal 2 — peer A:**

```powershell
python peerCommunicatorUDP.py --peer-id A --udp-port 6789 --rpc-port 5679 --drop-first-data-every 2 --drop-first-ack-every 3
```

**Terminal 3 — peer B:**

```powershell
python peerCommunicatorUDP.py --peer-id B --udp-port 6790 --rpc-port 5681 --drop-first-data-every 2 --drop-first-ack-every 3
```

**Terminal 4 — peer C:**

```powershell
python peerCommunicatorUDP.py --peer-id C --udp-port 6791 --rpc-port 5682 --drop-first-data-every 2 --drop-first-ack-every 3
```

**Terminal 5 — coordenador:**

```powershell
python comparisonServer.py --bind 127.0.0.1 --peers 3 --messages 8 --timeout 40
```

O coordenador imprime, entre outras linhas:

```text
[coordinator] FIFO valid: True; complete: True; cross-peer differences (NOT FIFO violations): ...
[coordinator] total UDP retransmissions: ...
[coordinator] saved: logs/round_<id>.json
```

O número de divergências globais e de retransmissões pode variar. Não é necessário que o número de divergências globais seja zero para que FIFO esteja correto.

**Sem perda simulada:** retire `--drop-first-data-every ...` e `--drop-first-ack-every ...` dos comandos. **Mais mensagens:** altere `--messages 8` para `--messages 50`, por exemplo. Depois, interrompa cada processo com `Ctrl+C`.

## 2. Arquitetura

```text
                    [ GroupMngr.py ]
                      XML-RPC :5680
                             ^
                registro / consulta dos peers
                             |
        +--------------------+--------------------+
        |                    |                    |
      [Peer A]             [Peer B]             [Peer C]
     UDP :6789            UDP :6790            UDP :6791
     RPC :5679            RPC :5681            RPC :5682
        |<----------- UDP DATA / ACK / FIN ----------->|
        |                    |                    |
        +--------------------+--------------------+
             ^             |              ^
             | XML-RPC     | XML-RPC      | XML-RPC
             +-------------v--------------+
                 [comparisonServer.py]
                     XML-RPC :5678
             prepara, inicia e recebe logs
```

### Principais componentes

| Arquivo | Função |
| --- | --- |
| `GroupMngr.py` | Registro, descoberta e remoção de peers via servidor XML-RPC. |
| `comparisonServer.py` | Orquestra rodadas usando RPC, recebe logs por RPC e valida FIFO, completude e divergência global. |
| `peerCommunicatorUDP.py` | Mantém servidor RPC de controle e socket UDP para troca de mensagens com outros peers. |
| `reliable_multicast.py` | Estado da rodada, ACK, retransmissões, números de sequência, hold-back queue e descarte de duplicatas. |
| `rpc_utils.py` | Utilitários de XML-RPC, timeout de cliente e servidor concorrente. |
| `constMP.py` | Endereços e portas padrão (alteráveis pela CLI). |
| `tests/` | Testes unitários e teste integrado de três processos reais com perdas. |

### Ciclo de uma rodada

1. Cada peer se registra no `GroupMngr` usando RPC (`register_peer`). O registro inclui **IP e portas UDP/RPC**, pois peers podem compartilhar o mesmo host.
2. O coordenador consulta a lista (`list_peers`) e envia `prepare_round` por RPC a **todos os peers**. Isso prepara os receptores e fixa a lista de participantes da rodada.
3. Após todos confirmarem a preparação, o coordenador envia `start_round` por RPC a cada peer; os métodos retornam rapidamente e o envio continua em *threads* locais.
4. Cada peer gera mensagens `(emissor, sequência)` e as envia **por sockets UDP** a todos os demais. O próprio emissor registra uma cópia local.
5. Cada receptor confirma via UDP (`ACK`) mensagens válidas, inclusive duplicatas. O emissor mantém uma tabela de mensagens pendentes por destinatário e retransmite periodicamente as que ainda não receberam ACK.
6. Quando termina o envio, o emissor transmite `FIN` (por UDP com ACK/retransmissão). O receptor **só considera completa** a comunicação depois de receber `FIN` de todos **e** todas as sequências de cada emissor, de `0` a `n_msgs - 1`.
7. Depois de terminar seu envio e receber todas as mensagens, cada peer envia seu log ao coordenador por RPC (`submit_log`). O coordenador compara os logs e grava JSON em `logs/`.

## 3. Parte 1A — substituição dos sockets cliente-servidor por RPC

**Antes:** `GroupMngr.py`, `comparisonServer.py` e os peers criavam sockets TCP manualmente, enviavam `pickle.dumps` e liam com `recv`. Era necessário gerenciar serialização, delimitação das mensagens, conexões e respostas. `recv` isolado não garante a leitura integral de um objeto TCP; `pickle.loads` sobre dados da rede também é inseguro diante de emissores não confiáveis.

**Depois:** as interações cliente-servidor chamam métodos remotos, usando `xmlrpc.server` e `xmlrpc.client` da biblioteca padrão. Exemplos: `register_peer(...)`, `list_peers()`, `prepare_round(...)`, `start_round(...)`, `submit_log(...)`. A infraestrutura RPC usa **HTTP sobre TCP por baixo**; eliminamos o uso **manual** dos sockets TCP nesses fluxos, não o protocolo TCP em si.

| Critério | TCP manual (original) | RPC sobre HTTP (implementado) |
| --- | --- | --- |
| Modelo | Envio de bytes e protocolo construído manualmente | Chamada de métodos remotos com argumentos e retorno |
| Complexidade | Exige serialização, framing, interpretação de comandos e tratamento manual | Biblioteca fornece formato de requisição, transporte e interpretação |
| Legibilidade / evolução | Mudanças de mensagens exigem alterações em múltiplos pontos | Operações nomeadas e interface cliente-servidor mais explícita |
| Interoperabilidade | Esquema privado com `pickle` Python | XML-RPC é padronizado e pode interoperar com outras linguagens |
| Desempenho | Menor overhead potencial com protocolo binário otimizado | Mais overhead por HTTP/XML e conversão de tipos |
| Segurança | `pickle` de rede permite desserialização perigosa | Evita `pickle`, mas XML-RPC **não substitui** autenticação/TLS |
| Tratamento de falhas | Precisa ser implementado manualmente | Erros de transporte e *faults* são expostos ao cliente; falhas ainda precisam ser tratadas |

**Discussão:** RPC melhora a organização do **plano de controle**, em que as operações são menos frequentes e a facilidade de manutenção tem grande valor. O **plano de dados** (multicast de mensagens) permanece UDP, preservando a possibilidade de atraso, perda e reordenação que o trabalho quer estudar. Isto é uma análise qualitativa, **não** uma medição de desempenho TCP versus RPC.

## 4. Parte 1B — confiabilidade e ordenação FIFO

**Pacotes UDP do protocolo:**

- `DATA`: `{"type":"DATA","round":"...","src":"A","seq":2}`
- `ACK`: `{"type":"ACK","round":"...","src":"B","ack_type":"DATA","seq":2}`
- `FIN`: `{"type":"FIN","round":"...","src":"A","count":8}` (também confirmado por ACK com `ack_type=FIN`, `seq=-1`).

O identificador `round` impede que mensagens antigas sejam confundidas com mensagens de uma rodada nova. Cada mensagem é identificada pelo par **(emissor, número de sequência)** dentro de uma rodada.

**Perda:** o emissor guarda cada mensagem pendente de confirmação, **para cada destino**, e a retransmite se o ACK não chegar após 150 ms (configurável por `--retry-ms`). Perder o `DATA` ou perder seu `ACK` provoca retransmissão; receber duplicatas é normal nesse protocolo. Há limite de tentativas (`--max-attempts`, padrão 80): se um destino permanecer indisponível, ocorre erro/timeout, **não uma promessa impossível de entrega garantida**.

**FIFO:** cada receptor mantém `next_seq[emissor]` e uma fila de espera `holdback[emissor]`. Uma mensagem fora de ordem é armazenada, mas só entregue quando todas as sequências anteriores daquele mesmo emissor tiverem chegado. Mensagens repetidas são confirmadas novamente, **sem segunda entrega**.

Exemplo: chegam de A as sequências `[2, 0, 1]`. A sequência `2` espera. A sequência `0` é entregue; quando `1` chega, o receptor entrega `1` e `2`. **Log final:** `[0, 1, 2]`.

**Encerramento:** `FIN` **não** encerra o receptor imediatamente. Ele traz o número esperado de mensagens do emissor e também é confirmado/retransmitido. O peer só fecha uma rodada após completar os fluxos de todos os emissores, confirmar sua transmissão e enviar o log por RPC. Assim, não se depende de um único datagrama de parada.

### FIFO não é ordenação total

- **FIFO:** A0 vem antes de A1 em **todos** os logs; B0 vem antes de B1. Está implementado nesta parte.
- **Ordem total:** todos recebem a **mesma intercalação** entre A0, B0, A1 etc. **Não** está implementado: isso pertence a etapas futuras.

O relatório calcula dois critérios independentes:

- `fifo_valid` / `complete`: requisitos que **precisam ser verdadeiros** na Parte 1.
- `divergent_positions`: posições em que os logs diferem **globalmente**. Pode ser maior que zero **mesmo se FIFO estiver perfeito**.

## 5. Testes e evidências

```powershell
python -m unittest discover -s tests -v
```

São **8 testes** no código entregue: buffer FIFO fora de ordem, descarte de duplicatas, `FIN` antes de dados, descarte determinístico de datagramas, validação de mensagens, distinção FIFO/ordem total, detecção de violação de FIFO e teste integrado real com três peers, perda de `DATA` e perda de `ACK`.

No teste integrado, cada peer envia 8 mensagens. Cada log deve conter **24 mensagens (3 × 8)**, com todos os pares `(emissor, seq)` presentes **uma única vez**. O relatório deve mostrar `fifo_valid=true`, `complete=true` e número de retransmissões **maior que zero**.

Para reproduzir os erros de rede, use `--drop-first-data-every 2` e `--drop-first-ack-every 3`. Essa injeção de falhas é **controlada e determinística**: descarta somente a **primeira ocorrência** de pacotes selecionados, permitindo evidenciar sua recuperação. Não equivale a teste de falha permanente de máquinas nem substitui uma avaliação em rede geograficamente distribuída.

O relatório `logs/round_<id>.json` inclui:

- Resumo de completude e FIFO por peer;
- Quantidade de retransmissões;
- Quantidade de posições diferentes entre logs (ordem global);
- Logs completos de recepção e estatísticas individuais de cada peer.

## 6. Configurações para múltiplas máquinas

Para implantar em VMs ou hosts distintos:

1. No host central, execute `GroupMngr.py --bind 0.0.0.0` e `comparisonServer.py --bind 0.0.0.0 --group-host <IP_CENTRAL> --peers 3 --messages 8`.
2. Em **cada** host de peer, execute `peerCommunicatorUDP.py --bind 0.0.0.0 --advertise-host <IP_PUBLICO_OU_ACESSIVEL> --group-host <IP_CENTRAL> --server-host <IP_CENTRAL> --peer-id A` (troque `A` para `B`, `C` etc.).
3. Libere UDP 6789 entre os peers e TCP 5678, 5680 e 5679 nas direções adequadas (RPC/HTTP). Em NAT, faça encaminhamento das portas e anuncie o IP/porta acessíveis. Se estiverem na mesma máquina, cada peer deve utilizar **portas UDP/RPC distintas**.

**Atenção:** XML-RPC e o protocolo UDP desta demonstração **não têm autenticação nem criptografia**. Execute apenas em `localhost`, laboratório ou rede privada isolada, ou proteja os servidores com regras de firewall/VPN. Não publique as portas irrestritamente na Internet.

## 7. Limitações / hipóteses explícitas

- Trata perda **transitória** (dados e ACKs), reordenação e duplicação dos pacotes. Não tolera falha permanente de nó nem partição infinita: há limite de retransmissões e timeout no coordenador.
- A rodada fixa seu conjunto de peers antes de começar: não implementa entrada/saída de membros durante uma rodada, persistência de logs dos peers nem recuperação de crash.
- Cada emissor conta mensagens desde zero em uma nova rodada. **Não** sincroniza relógios nem oferece ordem causal/total.
- Multicast é por **fan-out de UDP unicast**, assim como no exemplo original; não é multicast nativo IP.
- O teste automatizado é em **localhost**; não comprova desempenho ou escalabilidade em redes de múltiplas regiões.
- O programa não deve ser tratado como middleware de produção.

## 8. Sugestão de roteiro para o vídeo (os 3 integrantes)

**Integrante 1 — contexto e Parte 1A:** apresentar o problema do MPComm original, arquitetura e chamadas RPC. Abrir `GroupMngr.py`, `comparisonServer.py`, `rpc_utils.py`. Explicar por que XML-RPC sobre HTTP usa TCP **por baixo**, mas evita sockets TCP manuais.

**Integrante 2 — Parte 1B:** apresentar `reliable_multicast.py`, `next_seq`, `holdback`, `ACK` e retransmissões. Mostrar o caso de chegada `[2, 0, 1]` e por que só `[0, 1, 2]` é entregue.

**Integrante 3 — demonstração:** executar `python demo_local.py` e `python -m unittest discover -s tests -v`. Mostrar `fifo_valid=True`, `complete=True`, retransmissões positivas e abrir um arquivo `logs/round_<id>.json`. Encerrar esclarecendo que **diferenças de ordem global são esperadas** neste estágio.

Para a plataforma da disciplina, enviem a **URL do repositório do grupo**, verifiquem o **horário do último commit** e forneçam o **link do vídeo** segundo as regras acordadas entre vocês. Para as próximas partes, o enunciado pede **novos repositórios**; não acrescentar Partes 2–4 a este repositório de entrega sem orientação do professor.

## 9. Evidência de execução já registrada

A pasta `evidencias/` inclui um relatório JSON **real** de uma rodada local com **6 peers × 25 mensagens**, perdas artificiais de DATA/ACK, **150 mensagens em cada log**, **630 retransmissões**, `fifo_valid=true`, `complete=true` e **144 posições com diferenças entre logs**. Este último número é de ordem global, **não** de violações de FIFO. Os resultados podem variar em outras execuções.
