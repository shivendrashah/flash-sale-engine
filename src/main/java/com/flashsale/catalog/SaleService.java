package com.flashsale.catalog;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.flashsale.common.error.NotFoundException;

/**
 * Creates sales (admin), shows a sale with its remaining stock, and lists the newest sales (the shop).
 */
@Service
public class SaleService {

    private final SaleRepository sales;
    private final ProductRepository products;
    private final SaleItemRepository saleItems;
    private final Clock clock;

    public SaleService(SaleRepository sales, ProductRepository products, SaleItemRepository saleItems, Clock clock) {
        this.sales = sales;
        this.products = products;
        this.saleItems = saleItems;
        this.clock = clock;
    }

    @Transactional
    public SaleView createSale(CreateSaleRequest request) {
        Instant now = clock.instant();
        Sale sale = sales.save(new Sale(request.name(), request.startsAt(), request.endsAt(), request.perUserLimit(),
                request.admitPerSecond(), now));
        for (CreateSaleRequest.Item item : request.items()) {
            Product product = products.save(new Product(item.productName(), item.regularPrice(), now));
            saleItems.save(new SaleItem(sale.getId(), product.getId(), item.salePrice(), item.stock()));
        }
        return toView(sale);
    }

    /**
     * when Redis has no fresh copy. Tests and the admin API call it directly.
     */
    @Transactional(readOnly = true)
    public SaleView getSale(long saleId) {
        Sale sale = sales.findById(saleId).orElseThrow(() -> new NotFoundException("Sale", saleId));
        return toView(sale);
    }

    /**
     * page is light traffic next to the sale page and the hold step).
     */
    @Transactional(readOnly = true)
    public List<SaleView> listSales() {
        return sales.findTop20ByOrderByIdDesc().stream().map(this::toView).toList();
    }

    private SaleView toView(Sale sale) {
        List<SaleItem> items = saleItems.findBySaleIdOrderById(sale.getId());
        Map<Long, Product> productsById = products.findAllById(items.stream().map(SaleItem::getProductId).toList())
                .stream()
                .collect(Collectors.toMap(Product::getId, Function.identity()));
        List<SaleView.Item> itemViews = items.stream()
                .map(item -> {
                    Product product = productsById.get(item.getProductId());
                    return new SaleView.Item(item.getId(), product.getId(), product.getName(),
                            product.getRegularPrice(), item.getSalePrice(), item.getTotalStock(),
                            item.getAvailableStock());
                })
                .toList();
        return new SaleView(sale.getId(), sale.getName(), sale.getStartsAt(), sale.getEndsAt(),
                sale.isActiveAt(clock.instant()), sale.getPerUserLimit(), sale.getAdmitPerSecond(), itemViews);
    }
}
