// =====================================================================================================
// The shapes of the JSON the backend sends, written once as TypeScript types.
// Each one mirrors a Java record: SaleView, HoldView, PaymentView, QueueStatus, SaleStats and the Problem Details
// error (GlobalExceptionHandler). If a Java record changes, change it here too: TypeScript then shows every
// component that uses the old shape.
// =====================================================================================================

/** catalog/SaleView.java. Prices are BigDecimal in Java; Jackson writes them as JSON numbers. */
export interface Sale {
  id: number;
  name: string;
  startsAt: string;            // ISO-8601, e.g. "2026-10-11T10:00:00Z"
  endsAt: string;
  active: boolean;
  perUserLimit?: number;       // absent = no per-buyer limit
  admitPerSecond?: number;     // absent = no waiting room
  items: SaleItem[];
}

export interface SaleItem {
  saleItemId: number;
  productId: number;
  productName: string;
  regularPrice: number;
  salePrice: number;
  totalStock: number;
  availableStock: number;      // from the sale-page cache: up to ~1 s old
}

/** hold/HoldStatus.java: the hold's state machine. */
export type HoldStatus = 'HELD' | 'PAYMENT_PENDING' | 'CONFIRMED' | 'EXPIRED' | 'CANCELLED';

/** hold/HoldView.java */
export interface Hold {
  holdId: number;
  userId: string;
  saleId: number;
  status: HoldStatus;
  totalAmount: number;
  expiresAt: string;
  items: { saleItemId: number; quantity: number; unitPrice: number }[];
}

/** payment/PaymentView.java */
export interface Payment {
  paymentId: number;
  holdId: number;
  paymentStatus: 'PENDING' | 'SUCCEEDED' | 'FAILED';
  holdStatus: HoldStatus;
  amount: number;
  failureReason?: string | null;
  orderId?: number | null;
}

/** waitingroom/QueueStatus.java */
export interface QueueStatus {
  saleId: number;
  state: 'WAITING' | 'ADMITTED' | 'NOT_QUEUED';
  position?: number;
  estimatedWaitSeconds?: number;
}

/** demo/SaleStats.java */
export interface SaleStats {
  saleId: number;
  items: {
    saleItemId: number;
    productName: string;
    totalStock: number;
    availableStock: number;
    held: number;
    sold: number;
    oversold: number;
    mismatch: boolean;
  }[];
  orders: number;
  notified: number;
  inQueue: number;             // -1 = Redis could not say
  crowd: Record<string, number>;
}

/** The body of POST /api/admin/sales: catalog/CreateSaleRequest.java */
export interface CreateSale {
  name: string;
  startsAt: string;
  endsAt: string;
  perUserLimit?: number;
  admitPerSecond?: number;
  items: { productName: string; regularPrice: number; salePrice: number; stock: number }[];
}

/** Every error, from the app or from nginx: Problem Details (RFC 9457) with our "code". */
export interface Problem {
  status: number;
  title?: string;
  detail?: string;
  code?: string;
}
