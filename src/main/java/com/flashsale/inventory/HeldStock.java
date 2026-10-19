package com.flashsale.inventory;

import java.util.List;

/**
 * The stock one hold had set aside: the sale it belongs to, plus its lines. Giving stock back needs the sale
 * id because the Redis stock gate's keys contain it (StockGate.key: "stock:{saleId}:saleItemId").
 */
public record HeldStock(long saleId, List<StockLine> lines) {
}
