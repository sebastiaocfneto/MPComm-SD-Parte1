# Roteiro sugerido — vídeo da entrega Parte 1
## Equipe

- Islabelle Vitoria Mendonca Bastos — 202306973
- Felipe Sucupira de Oliveira — 202301439
- Sebastião Correa Fraga Neto — 202301487

Os papéis "Integrante 1", "Integrante 2" e "Integrante 3" abaixo são sugestões. A divisão das falas ainda será definida pelo grupo; a gravação não foi confirmada.

**Objetivo:** demonstração curta e verificável, com participação dos três integrantes. O enunciado não fixa duração, então o grupo pode ajustar a fala.

## Integrante 1 — introdução e RPC (aprox. 1min30)

> "Nosso ponto de partida é o MPComm do professor, um simulador de comunicação entre múltiplos processos. Na Parte 1, precisamos melhorar a comunicação cliente-servidor e garantir confiabilidade e FIFO no multicast. Antes, os clientes criavam manualmente sockets TCP e usavam `pickle` para trocar requisições com o gerente, o coordenador e os peers. Nós substituímos esse controle por XML-RPC usando recursos nativos do Python. Agora métodos como `register_peer`, `prepare_round`, `start_round` e `submit_log` são invocados remotamente. O RPC continua usando TCP por baixo, mas retira a necessidade de programar o protocolo de bytes diretamente. Ganham-se organização, clareza e interoperabilidade; em contrapartida, RPC tem sobrecarga de HTTP e serialização XML."

**Na tela:** `rpc_utils.py`, `GroupMngr.py`, `comparisonServer.py` e tabela da seção 3 do `README.md`.

## Integrante 2 — UDP confiável + FIFO (aprox. 1min30)

> "Para a troca de mensagens entre os peers, mantivemos sockets UDP, como exige o enunciado. UDP pode perder, duplicar ou reordenar mensagens, então cada emissor associa sequências às mensagens, guarda as confirmações pendentes e retransmite os pacotes que não receberam ACK. O receptor evita duplicatas. Para garantir FIFO, cada emissor tem seu próprio próximo número de sequência esperado e uma fila de espera. Se chegarem as mensagens dois, zero e um, a dois espera até que zero e um possam ser entregues. Além disso, nosso FIN não conclui a rodada até que todas as mensagens previstas tenham chegado. FIFO não significa ordem total: mensagens de emissores diferentes ainda podem ser intercaladas de formas diferentes entre os peers."

**Na tela:** `reliable_multicast.py`, função `receive_data`, `MulticastSender.run` e parte da seção 4 do `README.md`.

## Integrante 3 — demonstração (aprox. 2min)

**No terminal:**

```powershell
python -m unittest discover -s tests -v
python demo_local.py --peers 6 --messages 25
```

> "Aqui executamos os testes de unidade e integração com processos reais. A segunda execução sobe seis peers, usa RPC no controle e UDP para trocar mensagens, descartando de propósito o primeiro recebimento de alguns pacotes e alguns ACKs. O sistema retransmite os pacotes necessários. O coordenador confirma FIFO e entrega completa, e o JSON guarda os logs individuais. No exemplo previamente registrado, obtivemos 150 mensagens por peer e 630 retransmissões. Também existem diferenças de ordenação entre logs, mas são entre emissores diferentes e não violam FIFO; uma ordenação total só será considerada nas próximas partes."

**Na tela:** resultado `FIFO valid: True; complete: True`, retransmissões, pasta `logs/demo_...` e o arquivo JSON do experimento. Mostrar `summary` e um trecho de `logs`. Não há necessidade de exibir as 900 mensagens inteiras.

## Antes de subir o vídeo

- Confirmar a presença real dos três integrantes;
- Rodar o experimento sem erros na máquina do grupo;
- Confirmar URL do GitHub e horário do último commit;
- Inserir link do vídeo no README ou nas instruções de entrega, conforme orientação do professor;
- Evitar afirmar que foi implementada ordenação total, tolerância a crash ou um aplicativo com banco replicado: esses tópicos são de outras partes.
