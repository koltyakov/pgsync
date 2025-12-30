#!/usr/bin/env python3
"""
CRM Data Mutation Simulator
Simulates realistic data changes: inserts, updates, and deletes.
Runs continuously or for a specified duration to generate changes for sync testing.
"""

import argparse
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

import psycopg2
from faker import Faker

fake = Faker()

# Mutation rate profiles (operations per second)
RATE_PROFILES = {
    'slow': {'insert': 1, 'update': 2, 'delete': 0.1},
    'medium': {'insert': 5, 'update': 10, 'delete': 0.5},
    'fast': {'insert': 20, 'update': 50, 'delete': 2},
    'burst': {'insert': 100, 'update': 200, 'delete': 10},
}

# Tables and their mutation weights (higher = more frequent mutations)
TABLE_WEIGHTS = {
    'activities': 30,
    'email_logs': 25,
    'ticket_comments': 20,
    'tickets': 15,
    'contacts': 10,
    'leads': 10,
    'opportunities': 8,
    'organizations': 5,
    'deals': 5,
    'line_items': 5,
    'users': 2,
    'products': 2,
}

INDUSTRIES = [
    'Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail',
    'Education', 'Real Estate', 'Consulting', 'Marketing', 'Legal',
]

LEAD_SOURCES = [
    'Website', 'Referral', 'Cold Call', 'Trade Show', 'Social Media',
    'Email Campaign', 'Partner', 'Webinar', 'Advertisement', 'Other',
]


class MutationSimulator:
    def __init__(self, conn_string: str, rate_profile: str = 'medium'):
        self.conn = psycopg2.connect(conn_string)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.rates = RATE_PROFILES[rate_profile]
        self.running = True
        
        # Statistics
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0,
        }
        self.start_time = None
        
        # Cache IDs for references
        self._refresh_id_cache()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        print("\nReceived shutdown signal...")
        self.running = False
        
    def _refresh_id_cache(self):
        """Refresh cached IDs for foreign key references."""
        self.cache = {}
        
        tables_to_cache = [
            'users', 'teams', 'organizations', 'contacts', 'leads',
            'opportunities', 'deals', 'products', 'tickets', 'pipeline_stages',
            'product_categories', 'email_templates',
        ]
        
        for table in tables_to_cache:
            try:
                self.cursor.execute(f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 1000")
                self.cache[table] = [row[0] for row in self.cursor.fetchall()]
            except Exception:
                self.cache[table] = []
                
    def _get_random_id(self, table: str) -> Optional[str]:
        """Get a random ID from cache for the given table."""
        ids = self.cache.get(table, [])
        return random.choice(ids) if ids else None
        
    def run(self, duration: Optional[int] = None):
        """Run the mutation simulator."""
        self.start_time = datetime.now()
        end_time = self.start_time + timedelta(seconds=duration) if duration else None
        
        print(f"Starting mutation simulator with rates: {self.rates}")
        if duration:
            print(f"Running for {duration} seconds...")
        else:
            print("Running until stopped (Ctrl+C)...")
            
        # Calculate sleep intervals for each operation type
        intervals = {
            'insert': 1.0 / self.rates['insert'] if self.rates['insert'] > 0 else float('inf'),
            'update': 1.0 / self.rates['update'] if self.rates['update'] > 0 else float('inf'),
            'delete': 1.0 / self.rates['delete'] if self.rates['delete'] > 0 else float('inf'),
        }
        
        last_times = {op: time.time() for op in intervals}
        last_cache_refresh = time.time()
        last_stats_print = time.time()
        
        try:
            while self.running:
                if end_time and datetime.now() >= end_time:
                    break
                    
                current_time = time.time()
                
                # Refresh ID cache every 30 seconds
                if current_time - last_cache_refresh > 30:
                    self._refresh_id_cache()
                    last_cache_refresh = current_time
                    
                # Print stats every 10 seconds
                if current_time - last_stats_print > 10:
                    self._print_stats()
                    last_stats_print = current_time
                    
                # Execute operations based on their intervals
                for op, interval in intervals.items():
                    if current_time - last_times[op] >= interval:
                        try:
                            if op == 'insert':
                                self._do_insert()
                            elif op == 'update':
                                self._do_update()
                            elif op == 'delete':
                                self._do_delete()
                        except Exception as e:
                            self.stats['errors'] += 1
                            if self.stats['errors'] % 100 == 1:
                                print(f"Error during {op}: {e}")
                        last_times[op] = current_time
                        
                # Small sleep to prevent CPU spinning
                time.sleep(0.001)
                
        finally:
            self._print_final_stats()
            self.cursor.close()
            self.conn.close()
            
    def _print_stats(self):
        """Print current statistics."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        total_ops = self.stats['inserts'] + self.stats['updates'] + self.stats['deletes']
        rate = total_ops / elapsed if elapsed > 0 else 0
        
        print(f"[{elapsed:.0f}s] Inserts: {self.stats['inserts']}, "
              f"Updates: {self.stats['updates']}, Deletes: {self.stats['deletes']}, "
              f"Errors: {self.stats['errors']}, Rate: {rate:.1f} ops/s")
              
    def _print_final_stats(self):
        """Print final statistics."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        total_ops = self.stats['inserts'] + self.stats['updates'] + self.stats['deletes']
        rate = total_ops / elapsed if elapsed > 0 else 0
        
        print("\n" + "=" * 60)
        print("MUTATION SIMULATION COMPLETE")
        print("=" * 60)
        print(f"Duration: {elapsed:.1f} seconds")
        print(f"Total operations: {total_ops}")
        print(f"  - Inserts: {self.stats['inserts']}")
        print(f"  - Updates: {self.stats['updates']}")
        print(f"  - Deletes: {self.stats['deletes']}")
        print(f"  - Errors: {self.stats['errors']}")
        print(f"Average rate: {rate:.1f} ops/s")
        print("=" * 60)
        
    def _weighted_table_choice(self) -> str:
        """Choose a table based on weights."""
        tables = list(TABLE_WEIGHTS.keys())
        weights = list(TABLE_WEIGHTS.values())
        return random.choices(tables, weights=weights)[0]
        
    def _do_insert(self):
        """Perform a random insert operation."""
        table = self._weighted_table_choice()
        
        if table == 'activities':
            self._insert_activity()
        elif table == 'email_logs':
            self._insert_email_log()
        elif table == 'ticket_comments':
            self._insert_ticket_comment()
        elif table == 'tickets':
            self._insert_ticket()
        elif table == 'contacts':
            self._insert_contact()
        elif table == 'leads':
            self._insert_lead()
        elif table == 'opportunities':
            self._insert_opportunity()
        elif table == 'organizations':
            self._insert_organization()
        elif table == 'deals':
            self._insert_deal()
        elif table == 'line_items':
            self._insert_line_item()
        elif table == 'users':
            self._insert_user()
        elif table == 'products':
            self._insert_product()
            
        self.stats['inserts'] += 1
        
    def _do_update(self):
        """Perform a random update operation."""
        table = self._weighted_table_choice()
        record_id = self._get_random_id(table)
        
        if not record_id:
            return
            
        now = datetime.now()
        
        if table == 'activities':
            status = random.choice(['planned', 'in_progress', 'completed', 'cancelled'])
            self.cursor.execute(
                "UPDATE activities SET status = %s, updated_at = %s WHERE id = %s",
                (status, now, record_id)
            )
        elif table == 'email_logs':
            status = random.choice(['delivered', 'opened', 'clicked'])
            self.cursor.execute(
                "UPDATE email_logs SET status = %s, updated_at = %s WHERE id = %s",
                (status, now, record_id)
            )
        elif table == 'ticket_comments':
            self.cursor.execute(
                "UPDATE ticket_comments SET content = %s, updated_at = %s WHERE id = %s",
                (fake.paragraph(), now, record_id)
            )
        elif table == 'tickets':
            status = random.choice(['open', 'in_progress', 'pending', 'resolved', 'closed'])
            priority = random.choice(['low', 'medium', 'high', 'urgent'])
            self.cursor.execute(
                "UPDATE tickets SET status = %s, priority = %s, updated_at = %s WHERE id = %s",
                (status, priority, now, record_id)
            )
        elif table == 'contacts':
            self.cursor.execute(
                "UPDATE contacts SET phone = %s, job_title = %s, updated_at = %s WHERE id = %s",
                (fake.phone_number()[:50], fake.job(), now, record_id)
            )
        elif table == 'leads':
            status = random.choice(['new', 'contacted', 'qualified', 'unqualified'])
            score = random.randint(0, 100)
            self.cursor.execute(
                "UPDATE leads SET status = %s, score = %s, updated_at = %s WHERE id = %s",
                (status, score, now, record_id)
            )
        elif table == 'opportunities':
            stage_id = self._get_random_id('pipeline_stages')
            amount = Decimal(str(random.randint(5000, 500000)))
            self.cursor.execute(
                "UPDATE opportunities SET stage_id = %s, amount = %s, updated_at = %s WHERE id = %s",
                (stage_id, amount, now, record_id)
            )
        elif table == 'organizations':
            self.cursor.execute(
                "UPDATE organizations SET phone = %s, employee_count = %s, updated_at = %s WHERE id = %s",
                (fake.phone_number()[:50], random.randint(10, 10000), now, record_id)
            )
        elif table == 'deals':
            status = random.choice(['active', 'completed', 'cancelled'])
            self.cursor.execute(
                "UPDATE deals SET status = %s, updated_at = %s WHERE id = %s",
                (status, now, record_id)
            )
        elif table == 'line_items':
            quantity = Decimal(str(random.randint(1, 100)))
            self.cursor.execute(
                "UPDATE line_items SET quantity = %s, updated_at = %s WHERE id = %s",
                (quantity, now, record_id)
            )
        elif table == 'users':
            self.cursor.execute(
                "UPDATE users SET last_login_at = %s, updated_at = %s WHERE id = %s",
                (now, now, record_id)
            )
        elif table == 'products':
            stock = random.randint(0, 1000)
            self.cursor.execute(
                "UPDATE products SET stock_quantity = %s, updated_at = %s WHERE id = %s",
                (stock, now, record_id)
            )
            
        self.stats['updates'] += 1
        
    def _do_delete(self):
        """Perform a random delete operation."""
        # Only delete from less critical tables
        deletable_tables = ['activities', 'email_logs', 'ticket_comments', 'leads']
        table = random.choice(deletable_tables)
        
        # Get oldest records to delete (FIFO)
        self.cursor.execute(f"SELECT id FROM {table} ORDER BY created_at ASC LIMIT 1")
        row = self.cursor.fetchone()
        
        if row:
            self.cursor.execute(f"DELETE FROM {table} WHERE id = %s", (row[0],))
            self.stats['deletes'] += 1
            
    # Insert methods for each table
    def _insert_activity(self):
        now = datetime.now()
        activity_type = random.choice(['call', 'meeting', 'task', 'email', 'note'])
        # Use past or current time for start_time to ensure incremental sync works
        start_time = now - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23))
        self.cursor.execute("""
            INSERT INTO activities (id, type, subject, status, priority, start_time, 
                organization_id, contact_id, assigned_to, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()), activity_type, fake.sentence(),
            random.choice(['planned', 'in_progress', 'completed']),
            random.choice(['low', 'normal', 'high']),
            start_time,
            self._get_random_id('organizations'),
            self._get_random_id('contacts'),
            self._get_random_id('users'),
            now, now,
        ))
        
    def _insert_email_log(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO email_logs (id, contact_id, from_address, to_addresses, 
                subject, body, status, sent_at, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            self._get_random_id('contacts'),
            fake.company_email(),
            [fake.email()],
            fake.sentence(),
            fake.paragraph(),
            'sent',
            now,
            now, now,
        ))
        
    def _insert_ticket_comment(self):
        now = datetime.now()
        ticket_id = self._get_random_id('tickets')
        if not ticket_id:
            return
        self.cursor.execute("""
            INSERT INTO ticket_comments (id, ticket_id, author_id, author_type, 
                content, is_internal, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            ticket_id,
            self._get_random_id('users'),
            random.choice(['user', 'customer']),
            fake.paragraph(),
            random.random() > 0.7,
            now, now,
        ))
        
    def _insert_ticket(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO tickets (id, organization_id, contact_id, subject, 
                description, status, priority, type, category, channel, 
                assigned_to, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            self._get_random_id('organizations'),
            self._get_random_id('contacts'),
            fake.sentence(),
            fake.paragraph(),
            'open',
            random.choice(['low', 'medium', 'high', 'urgent']),
            random.choice(['bug', 'question', 'task']),
            random.choice(['Technical', 'Billing', 'Sales', 'General']),
            random.choice(['email', 'phone', 'web', 'chat']),
            self._get_random_id('users'),
            now, now,
        ))
        
    def _insert_contact(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO contacts (id, organization_id, first_name, last_name, 
                email, phone, job_title, department, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            self._get_random_id('organizations'),
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number()[:50],
            fake.job(),
            random.choice(['Sales', 'Marketing', 'Engineering', 'Finance']),
            'active',
            now, now,
        ))
        
    def _insert_lead(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO leads (id, first_name, last_name, email, phone, 
                company_name, job_title, industry, lead_source, status, 
                score, assigned_to, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number()[:50],
            fake.company(),
            fake.job(),
            random.choice(INDUSTRIES),
            random.choice(LEAD_SOURCES),
            'new',
            random.randint(0, 100),
            self._get_random_id('users'),
            now, now,
        ))
        
    def _insert_opportunity(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO opportunities (id, name, organization_id, contact_id, 
                stage_id, amount, status, type, assigned_to, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            f"{fake.company()} - New Business",
            self._get_random_id('organizations'),
            self._get_random_id('contacts'),
            self._get_random_id('pipeline_stages'),
            Decimal(str(random.randint(5000, 500000))),
            'open',
            random.choice(['New Business', 'Expansion', 'Renewal']),
            self._get_random_id('users'),
            now, now,
        ))
        
    def _insert_organization(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO organizations (id, name, industry, website, phone, 
                email, city, state, country, employee_count, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            fake.company(),
            random.choice(INDUSTRIES),
            fake.url(),
            fake.phone_number()[:50],
            fake.company_email(),
            fake.city(),
            fake.state_abbr(),
            'USA',
            random.randint(10, 10000),
            'active',
            now, now,
        ))
        
    def _insert_deal(self):
        now = datetime.now()
        start_date = now.date() + timedelta(days=random.randint(0, 30))
        self.cursor.execute("""
            INSERT INTO deals (id, organization_id, contact_id, name, amount, 
                status, start_date, end_date, contract_number, assigned_to, 
                created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            self._get_random_id('organizations'),
            self._get_random_id('contacts'),
            f"Deal - {fake.company()}",
            Decimal(str(random.randint(10000, 500000))),
            'active',
            start_date,
            start_date + timedelta(days=random.randint(30, 365)),
            f"CNT-{random.randint(10000, 99999)}",
            self._get_random_id('users'),
            now, now,
        ))
        
    def _insert_line_item(self):
        deal_id = self._get_random_id('deals')
        product_id = self._get_random_id('products')
        if not deal_id:
            return
        now = datetime.now()
        quantity = Decimal(str(random.randint(1, 100)))
        unit_price = Decimal(str(random.randint(100, 10000)))
        subtotal = quantity * unit_price
        self.cursor.execute("""
            INSERT INTO line_items (id, deal_id, product_id, description, 
                quantity, unit_price, subtotal, total, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            deal_id,
            product_id,
            fake.sentence(),
            quantity,
            unit_price,
            subtotal,
            subtotal,
            now, now,
        ))
        
    def _insert_user(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO users (id, email, first_name, last_name, phone, 
                role, department, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            fake.unique.email(),
            fake.first_name(),
            fake.last_name(),
            fake.phone_number()[:50],
            random.choice(['admin', 'manager', 'user']),
            random.choice(['Sales', 'Marketing', 'Support', 'Engineering']),
            True,
            now, now,
        ))
        
    def _insert_product(self):
        now = datetime.now()
        self.cursor.execute("""
            INSERT INTO products (id, category_id, sku, name, description, 
                unit_price, is_active, stock_quantity, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            self._get_random_id('product_categories'),
            f"SKU-{random.randint(10000, 99999)}",
            f"{fake.word().title()} {random.choice(['Pro', 'Plus', 'Enterprise', 'Basic'])}",
            fake.paragraph(),
            Decimal(str(random.randint(100, 50000))),
            True,
            random.randint(0, 1000),
            now, now,
        ))


def main():
    parser = argparse.ArgumentParser(description='Simulate CRM data mutations')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5433, help='Database port')
    parser.add_argument('--database', default='crm_source', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='postgres', help='Database password')
    parser.add_argument('--rate', choices=RATE_PROFILES.keys(), default='medium',
                       help='Mutation rate profile: slow, medium, fast, burst')
    parser.add_argument('--duration', type=int, default=None,
                       help='Duration in seconds (default: run until Ctrl+C)')
    
    args = parser.parse_args()
    
    conn_string = f"host={args.host} port={args.port} dbname={args.database} user={args.user} password={args.password}"
    
    print(f"Connecting to {args.database} on {args.host}:{args.port}")
    print(f"Rate profile: {args.rate} - {RATE_PROFILES[args.rate]}")
    
    simulator = MutationSimulator(conn_string, args.rate)
    simulator.run(args.duration)


if __name__ == '__main__':
    main()
