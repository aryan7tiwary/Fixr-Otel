import requests
import time
import random
import threading
import uuid

BASE_URL = "http://localhost:8080"
NUM_USERS = 8
MIN_WAIT = 1.0
MAX_WAIT = 3.0

PRODUCTS = [
    "0PUK6V6EV0", "1YMWWN1N4O", "2ZYFJ3GM2N", "66VCHSJNUP", "6E92ZMYYFZ", 
    "9SIQT8TOJO", "L9ECAV7KIM", "LS4PSXUNUM", "OLJCESPC7Z", "HQTGWGPNH4"
]

def simulate_user(user_id):
    """Simulates a single user navigating the storefront."""
    session = requests.Session()
    user_uuid = str(uuid.uuid4())
    print(f"[User {user_id}] Started browsing session.")
    
    while True:
        try:
            # Added "empty_cart" to trigger the explicit 500 error from the flagd toggle
            action = random.choices(
                ["browse_home", "browse_products", "add_to_cart", "checkout", "empty_cart"],
                weights=[10, 10, 10, 50, 20], 
                k=1
            )[0]
            
            if action == "browse_home":
                response = session.get(f"{BASE_URL}/", timeout=5)
                print(f"[User {user_id}] Browsed homepage - Status: {response.status_code}")
                
            elif action == "browse_products":
                product_id = random.choice(PRODUCTS)
                response = session.get(f"{BASE_URL}/api/products/{product_id}", timeout=5)
                print(f"[User {user_id}] Browsed product {product_id} - Status: {response.status_code}")
                
            elif action == "add_to_cart":
                payload = {
                    "item": {"productId": random.choice(PRODUCTS), "quantity": random.randint(1, 3)},
                    "userId": user_uuid
                }
                response = session.post(f"{BASE_URL}/api/cart", json=payload, timeout=5)
                if response.status_code >= 500:
                    print(f"[User {user_id}] Add to cart failed - Status: {response.status_code}")
                else:
                    print(f"[User {user_id}] Added item to cart - Status: {response.status_code}")
                    
            elif action == "empty_cart":
                # This explicitly hits the EmptyCart RPC handler which flagd breaks
                payload = {"userId": user_uuid}
                response = session.delete(f"{BASE_URL}/api/cart", json=payload, timeout=5)
                if response.status_code >= 500:
                    print(f"[User {user_id}] Empty cart failed - Status: {response.status_code} (FLAGD FAULT DETECTED!)")
                else:
                    print(f"[User {user_id}] Emptied cart - Status: {response.status_code}")
                
            elif action == "checkout":
                payload_cart = {"item": {"productId": random.choice(PRODUCTS), "quantity": 1}, "userId": user_uuid}
                session.post(f"{BASE_URL}/api/cart", json=payload_cart, timeout=5)
                
                payload = {
                    "email": f"user{user_id}@example.com",
                    "address": {
                        "streetAddress": "1600 Amphitheatre Parkway",
                        "city": "Mountain View",
                        "state": "CA",
                        "country": "United States",
                        "zipCode": "94043"
                    },
                    "userCurrency": "USD",
                    "creditCard": {
                        "creditCardNumber": "4432-8015-6152-0454",
                        "creditCardExpirationMonth": 1,
                        "creditCardExpirationYear": 2039,
                        "creditCardCvv": 672
                    },
                    "userId": user_uuid
                }
                response = session.post(f"{BASE_URL}/api/checkout", json=payload, timeout=5)
                if response.status_code >= 500:
                    print(f"[User {user_id}] Checkout failed - Status: {response.status_code}")
                else:
                    print(f"[User {user_id}] Checkout complete - Status: {response.status_code}")
                    user_uuid = str(uuid.uuid4())
                    
        except requests.exceptions.RequestException as e:
            print(f"[User {user_id}] Connection error: {type(e).__name__}")
            
        time.sleep(random.uniform(MIN_WAIT, MAX_WAIT))

def main():
    print(f"Starting custom load generator against {BASE_URL} with {NUM_USERS} users...")
    print("Press Ctrl+C to stop.\n")
    
    threads = []
    for i in range(1, NUM_USERS + 1):
        t = threading.Thread(target=simulate_user, args=(i,), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(random.uniform(0.1, 0.5))
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[System] Load generator stopped gracefully.")

if __name__ == "__main__":
    main()
