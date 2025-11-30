import psycopg2
from faker import Faker
import time
import random
import os
# Tạo data "Tĩnh": Tự động INSERT 500 khách hàng (customers) và 100 sản phẩm (products) khi nó khởi động lần đầu.
# Tạo data "Động": Chạy vòng lặp while True để liên tục mô phỏng:
# 80% thời gian: Tạo một đơn hàng mới (orders) với 1-5 món hàng (order_items).
# 20% thời gian: Cập nhật một đơn hàng cũ (ví dụ: đổi status từ processing -> shipped). (Cái này RẤT QUAN TRỌNG để Debezium bắt được event UPDATE).
# === 1. KẾT NỐI DATABASE ===
# Sửa "postgres1" thành "postgres" (tên service trong docker-compose)
DB_HOST = os.getenv("DB_HOST", "postgres") 
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "testdb")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123")

fake = Faker()

CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Beauty', 'Toys', 'Sports']
def get_connection():
    """Hàm helper để lấy kết nối DB"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

# === 2. TẠO DATA "TĨNH" (Sản phẩm & Khách hàng) ===
def populate_static_data():
    """
    Chạy 1 lần duy nhất khi khởi động.
    Tạo 100 sản phẩm và 500 khách hàng để làm "nguyên liệu".
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Kiểm tra xem đã có data chưa, nếu có thì bỏ qua
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] > 0:
            print("✅ Static data (customers, products) already exists. Skipping population.")
            cur.close()
            conn.close()
            return

        print("⏳ Populating static data (products and customers)...")
        
        # Tạo 100 sản phẩm (products)
        products_data = []
        for _ in range(100):
            # SỬA ĐOẠN NÀY:
            product_name = f"{fake.word().title()} {fake.word().title()}" # Tạo tên giả: "Table Red"
            category = random.choice(CATEGORIES) # Chọn random từ danh sách trên
            price = round(random.uniform(10, 500), 2)
            
            products_data.append((product_name, category, price))
            
        cur.executemany(
            "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
            products_data
        )

        # Tạo 500 khách hàng (customers)
        customers_data = []
        for _ in range(500):
            customers_data.append((
                fake.name(),
                fake.email(),
                fake.address().replace('\n', ', ')
            ))
        cur.executemany(
            "INSERT INTO customers (name, email, address) VALUES (%s, %s, %s)",
            customers_data
        )

        conn.commit()
        print(f"✅ Populated {len(products_data)} products and {len(customers_data)} customers.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error in populate_static_data: {e}")

# === 3. TẠO DATA "ĐỘNG" (Giao dịch) ===
def simulate_new_order():
    """
    Mô phỏng 1 giao dịch MỚI:
    1. Tạo 1 'orders' (đơn hàng).
    2. Tạo 1-5 'order_items' (món hàng trong giỏ) cho đơn hàng đó.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 1. Chọn ngẫu nhiên 1 khách hàng
        cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
        res = cur.fetchone()
        if res is None:
            print("⚠️ No customers found! Skipping order generation.")
            return # Thoát hàm nếu không có khách
        customer_id = res[0]
        # 2. Tạo đơn hàng (Bảng 'orders')
        # Dùng "RETURNING id" để lấy ngay order_id vừa tạo
        cur.execute(
            "INSERT INTO orders (customer_id, status) VALUES (%s, %s) RETURNING id;",
            (customer_id, 'processing')
        )
        order_id = cur.fetchone()[0]

        # 3. Tạo giỏ hàng (Bảng 'order_items')
        num_items = random.randint(1, 5) # Đơn hàng có từ 1-5 món
        total_amount = 0
        
        for _ in range(num_items):
            # Lấy ngẫu nhiên 1 sản phẩm và số lượng
            cur.execute("SELECT id, price FROM products ORDER BY RANDOM() LIMIT 1")
            product_id, price = cur.fetchone()
            quantity = random.randint(1, 3)
            
            total_amount += (price * quantity)

            # Chèn vào bảng chi tiết đơn hàng
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s);",
                (order_id, product_id, quantity)
            )
        
        # (Tùy chọn) Cập nhật tổng tiền vào bảng 'orders'
        cur.execute("UPDATE orders SET total_amount = %s WHERE id = %s;", (round(total_amount, 2), order_id))

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ [NEW ORDER] ID: {order_id}, Customer: {customer_id}, {num_items} items, Total: ${total_amount:.2f}")

    except Exception as e:
        print(f"❌ Error in simulate_new_order: {e}")


def simulate_order_update():
    """
    Mô phỏng 1 CẬP NHẬT đơn hàng (để Debezium bắt event 'UPDATE')
    Chọn 1 đơn 'processing' và đổi nó thành 'shipped' hoặc 'cancelled'.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 1. Tìm 1 đơn hàng 'processing'
        cur.execute("SELECT id FROM orders WHERE status = 'processing' ORDER BY RANDOM() LIMIT 1")
        order_to_update = cur.fetchone()

        if order_to_update:
            order_id = order_to_update[0]
            new_status = random.choice(['shipped', 'cancelled', 'delivered'])
            
            # 2. Cập nhật status
            cur.execute(
                "UPDATE orders SET status = %s WHERE id = %s;",
                (new_status, order_id)
            )
            conn.commit()
            print(f"🚚 [UPDATE ORDER] ID: {order_id} status changed to '{new_status}'")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error in simulate_order_update: {e}")


# === 4. CHẠY CHÍNH ===
if __name__ == "__main__":
    # Chạy 1 lần duy nhất để tạo data tĩnh
    populate_static_data()
    
    # Vòng lặp vô hạn mô phỏng giao dịch
    print("🚀 Starting real-time data simulation...")
    while True:
        try:
            # 80% thời gian tạo đơn mới, 20% cập nhật đơn cũ
            if random.random() < 0.8:
                simulate_new_order()
            else:
                simulate_order_update()
                
            # Nghỉ ngẫu nhiên 1-5 giây
            time.sleep(random.uniform(1, 5)) 

        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            time.sleep(5)