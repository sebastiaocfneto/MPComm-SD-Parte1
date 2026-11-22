# Evidências reproduzíveis

O arquivo `rodada_6_peers_com_perdas.json` registra uma execução real em localhost, gerada com:

```bash
python demo_local.py --peers 6 --messages 25 --drop-first-data-every 2 --drop-first-ack-every 3
```

- 6 processos peer × 25 mensagens: 150 mensagens únicas esperadas por log;
- Todos os 6 logs: 150 mensagens recebidas, sem ausência nem duplicação;
- FIFO: `true`;
- Entrega completa: `true`;
- Retransmissões UDP: `630`;
- Divergência de interleaving entre logs: `144` posições (não viola FIFO).

**Contexto:** experiência local com perdas artificiais *uma vez por pacote selecionado*.
Não demonstra tolerância a falhas de processo nem ordenação total, que não fazem parte da Parte 1.
