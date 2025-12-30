#!/usr/bin/env python3
"""
CRM Data Seeder
Generates realistic test data for the CRM database.
Supports configurable data volumes from hundreds to millions of rows.
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker()

# Data volume profiles
PROFILES = {
    'small': {
        'users': 10,
        'teams': 3,
        'organizations': 100,
        'contacts': 500,
        'leads': 200,
        'opportunities': 150,
        'deals': 50,
        'products': 50,
        'tickets': 200,
        'activities': 1000,
        'email_logs': 500,
    },
    'medium': {
        'users': 50,
        'teams': 10,
        'organizations': 5000,
        'contacts': 25000,
        'leads': 10000,
        'opportunities': 5000,
        'deals': 2000,
        'products': 200,
        'tickets': 10000,
        'activities': 50000,
        'email_logs': 25000,
    },
    'large': {
        'users': 200,
        'teams': 30,
        'organizations': 50000,
        'contacts': 250000,
        'leads': 100000,
        'opportunities': 50000,
        'deals': 20000,
        'products': 500,
        'tickets': 100000,
        'activities': 500000,
        'email_logs': 250000,
    },
    'xlarge': {
        'users': 500,
        'teams': 50,
        'organizations': 200000,
        'contacts': 1000000,
        'leads': 500000,
        'opportunities': 200000,
        'deals': 80000,
        'products': 1000,
        'tickets': 500000,
        'activities': 2000000,
        'email_logs': 1000000,
    },
}

INDUSTRIES = [
    'Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail',
    'Education', 'Real Estate', 'Consulting', 'Marketing', 'Legal',
    'Transportation', 'Energy', 'Hospitality', 'Media', 'Telecommunications',
]

LEAD_SOURCES = [
    'Website', 'Referral', 'Cold Call', 'Trade Show', 'Social Media',
    'Email Campaign', 'Partner', 'Webinar', 'Advertisement', 'Other',
]

LEAD_STATUSES = ['new', 'contacted', 'qualified', 'unqualified', 'converted']
TICKET_STATUSES = ['open', 'in_progress', 'pending', 'resolved', 'closed']
TICKET_PRIORITIES = ['low', 'medium', 'high', 'urgent']
TICKET_CATEGORIES = ['Technical', 'Billing', 'Sales', 'General', 'Feature Request']
ACTIVITY_TYPES = ['call', 'meeting', 'task', 'email', 'note']
ACTIVITY_STATUSES = ['planned', 'in_progress', 'completed', 'cancelled']

class DataSeeder:
    def __init__(self, conn_string: str, profile: str = 'small', batch_size: int = 1000):
        self.conn = psycopg2.connect(conn_string)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        self.profile = PROFILES[profile]
        self.batch_size = batch_size
        
        # Cache for foreign key references
        self.user_ids = []
        self.team_ids = []
        self.org_ids = []
        self.contact_ids = []
        self.lead_ids = []
        self.opportunity_ids = []
        self.deal_ids = []
        self.product_ids = []
        self.ticket_ids = []
        self.stage_ids = []
        self.category_ids = []
        self.price_book_id = None
        self.template_ids = []
        
    def run(self):
        """Execute full data seeding process."""
        print(f"Starting data seeding with profile: {self.profile}")
        start = datetime.now()
        
        try:
            self._seed_reference_data()
            self._load_existing_ids()
            self._seed_users()
            self._seed_teams()
            self._seed_team_members()
            self._seed_organizations()
            self._seed_contacts()
            self._seed_leads()
            self._seed_opportunities()
            self._seed_deals()
            self._seed_products()
            self._seed_price_book_entries()
            self._seed_line_items()
            self._seed_tickets()
            self._seed_ticket_comments()
            self._seed_activities()
            self._seed_email_templates()
            self._seed_email_logs()
            
            self.conn.commit()
            elapsed = datetime.now() - start
            print(f"\nData seeding completed in {elapsed.total_seconds():.2f}s")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error: {e}")
            raise
        finally:
            self.cursor.close()
            self.conn.close()
    
    def _seed_reference_data(self):
        """Seed reference/lookup tables that other tables depend on."""
        print("Seeding reference data...")
        
        # Pipeline Stages
        stages = [
            ('Prospecting', 'Initial contact and qualification', 1, 10.00),
            ('Qualification', 'Determining fit and budget', 2, 20.00),
            ('Proposal', 'Presenting solution and pricing', 3, 40.00),
            ('Negotiation', 'Contract and terms discussion', 4, 60.00),
            ('Closed Won', 'Deal successfully closed', 5, 100.00),
            ('Closed Lost', 'Deal lost to competitor or no decision', 6, 0.00),
        ]
        execute_values(self.cursor, """
            INSERT INTO pipeline_stages (name, description, display_order, probability)
            VALUES %s
        """, stages)
        
        # Product Categories
        categories = [
            ('Software', None, 'Software products and licenses', 1),
            ('Hardware', None, 'Physical equipment and devices', 2),
            ('Services', None, 'Professional and consulting services', 3),
            ('Support', None, 'Maintenance and support plans', 4),
            ('Training', None, 'Training and education services', 5),
        ]
        execute_values(self.cursor, """
            INSERT INTO product_categories (name, parent_id, description, display_order)
            VALUES %s
        """, categories)
        
        # Price Books
        self.cursor.execute("""
            INSERT INTO price_books (name, description, is_default, is_active)
            VALUES 
                ('Standard', 'Standard pricing for all customers', true, true),
                ('Enterprise', 'Discounted pricing for enterprise customers', false, true),
                ('Partner', 'Special pricing for partners', false, true)
        """)
            
    def _load_existing_ids(self):
        """Load existing reference IDs from database."""
        print("Loading existing reference data...")
        
        self.cursor.execute("SELECT id FROM pipeline_stages ORDER BY display_order")
        self.stage_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT id FROM product_categories")
        self.category_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT id FROM price_books WHERE is_default = true LIMIT 1")
        row = self.cursor.fetchone()
        if row:
            self.price_book_id = row[0]
            
    def _batch_insert(self, table: str, columns: list, data: list, returning: str = None):
        """Insert data in batches with optional returning."""
        if not data:
            return []
            
        cols = ', '.join(columns)
        template = '(' + ', '.join(['%s'] * len(columns)) + ')'
        
        returned_ids = []
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            if returning:
                query = f"INSERT INTO {table} ({cols}) VALUES %s RETURNING {returning}"
                result = execute_values(self.cursor, query, batch, template=template, fetch=True)
                returned_ids.extend([row[0] for row in result])
            else:
                query = f"INSERT INTO {table} ({cols}) VALUES %s"
                execute_values(self.cursor, query, batch, template=template)
                
        return returned_ids
        
    def _random_timestamp(self, days_back: int = 365) -> datetime:
        """Generate random timestamp within the past N days."""
        return datetime.now() - timedelta(
            days=random.randint(0, days_back),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
    
    def _updated_at(self, created: datetime, max_days: int = 30) -> datetime:
        """Generate an updated_at timestamp between created and now (never in future)."""
        now = datetime.now()
        max_delta = min((now - created).days, max_days)
        if max_delta <= 0:
            return created
        return created + timedelta(
            days=random.randint(0, max_delta),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
    def _seed_users(self):
        """Seed users table."""
        count = self.profile['users']
        print(f"Seeding {count} users...")
        
        roles = ['admin', 'manager', 'user', 'user', 'user']  # Weighted towards users
        departments = ['Sales', 'Marketing', 'Support', 'Engineering', 'Operations']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(365)
            data.append((
                str(uuid.uuid4()),
                fake.unique.email(),
                fake.first_name(),
                fake.last_name(),
                fake.phone_number()[:50],
                f"https://i.pravatar.cc/150?u={uuid.uuid4()}",
                random.choice(roles),
                random.choice(departments),
                True,
                self._random_timestamp(30) if random.random() > 0.3 else None,
                random.choice(['UTC', 'America/New_York', 'America/Los_Angeles', 'Europe/London']),
                'en-US',
                created,
                self._updated_at(created, 30),
            ))
            
        columns = [
            'id', 'email', 'first_name', 'last_name', 'phone', 'avatar_url',
            'role', 'department', 'is_active', 'last_login_at', 'timezone',
            'locale', 'created_at', 'updated_at'
        ]
        self.user_ids = self._batch_insert('users', columns, data, returning='id')
        
        # Update some users with managers
        if len(self.user_ids) > 1:
            managers = self.user_ids[:max(1, len(self.user_ids) // 5)]
            for user_id in self.user_ids[len(managers):]:
                if random.random() > 0.3:
                    self.cursor.execute(
                        "UPDATE users SET manager_id = %s WHERE id = %s",
                        (random.choice(managers), user_id)
                    )
                    
    def _seed_teams(self):
        """Seed teams table."""
        count = self.profile['teams']
        print(f"Seeding {count} teams...")
        
        team_names = [
            'Enterprise Sales', 'SMB Sales', 'Customer Success', 'Support Tier 1',
            'Support Tier 2', 'Marketing Ops', 'SDR Team', 'Account Management',
            'Partner Sales', 'Implementation', 'Training', 'Product Marketing',
        ]
        
        data = []
        for i in range(min(count, len(team_names))):
            created = self._random_timestamp(365)
            data.append((
                str(uuid.uuid4()),
                team_names[i],
                fake.paragraph(nb_sentences=2),
                random.choice(self.user_ids) if self.user_ids else None,
                True,
                created,
                created,
            ))
            
        columns = ['id', 'name', 'description', 'manager_id', 'is_active', 'created_at', 'updated_at']
        self.team_ids = self._batch_insert('teams', columns, data, returning='id')
        
    def _seed_team_members(self):
        """Seed team_members table."""
        if not self.team_ids or not self.user_ids:
            return
            
        print("Seeding team members...")
        
        data = []
        assigned_users = set()
        
        for team_id in self.team_ids:
            # Each team gets 3-10 members
            team_size = random.randint(3, min(10, len(self.user_ids)))
            available = [u for u in self.user_ids if u not in assigned_users]
            
            if len(available) < team_size:
                available = self.user_ids  # Allow users in multiple teams
                
            members = random.sample(available, team_size)
            
            for i, user_id in enumerate(members):
                role = 'lead' if i == 0 else 'member'
                created = self._random_timestamp(300)
                data.append((
                    str(uuid.uuid4()),
                    team_id,
                    user_id,
                    role,
                    created,
                    created,
                    created,
                ))
                assigned_users.add(user_id)
                
        columns = ['id', 'team_id', 'user_id', 'role', 'joined_at', 'created_at', 'updated_at']
        self._batch_insert('team_members', columns, data)
        
    def _seed_organizations(self):
        """Seed organizations table."""
        count = self.profile['organizations']
        print(f"Seeding {count} organizations...")
        
        statuses = ['active', 'active', 'active', 'inactive', 'prospect']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(730)
            data.append((
                str(uuid.uuid4()),
                fake.company(),
                random.choice(INDUSTRIES),
                fake.url(),
                fake.phone_number()[:50],
                fake.company_email(),
                fake.street_address(),
                fake.secondary_address() if random.random() > 0.7 else None,
                fake.city(),
                fake.state_abbr(),
                fake.postcode(),
                fake.country(),
                random.randint(10, 50000),
                Decimal(str(random.randint(100000, 100000000))),
                random.choice(statuses),
                random.choice(LEAD_SOURCES),
                created,
                self._updated_at(created, 60),
            ))
            
        columns = [
            'id', 'name', 'industry', 'website', 'phone', 'email',
            'address_line1', 'address_line2', 'city', 'state', 'postal_code',
            'country', 'employee_count', 'annual_revenue', 'status', 'source',
            'created_at', 'updated_at'
        ]
        self.org_ids = self._batch_insert('organizations', columns, data, returning='id')
        
    def _seed_contacts(self):
        """Seed contacts table."""
        count = self.profile['contacts']
        print(f"Seeding {count} contacts...")
        
        job_titles = [
            'CEO', 'CTO', 'CFO', 'VP of Sales', 'VP of Marketing', 'Director',
            'Manager', 'Engineer', 'Developer', 'Analyst', 'Consultant',
            'Coordinator', 'Specialist', 'Administrator', 'Executive',
        ]
        departments = ['Executive', 'Sales', 'Marketing', 'Engineering', 'Finance', 'Operations', 'HR', 'IT']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(365)
            first_name = fake.first_name()
            last_name = fake.last_name()
            data.append((
                str(uuid.uuid4()),
                random.choice(self.org_ids) if self.org_ids and random.random() > 0.1 else None,
                first_name,
                last_name,
                fake.email(),
                fake.phone_number()[:50],
                fake.phone_number()[:50] if random.random() > 0.5 else None,
                random.choice(job_titles),
                random.choice(departments),
                f"https://linkedin.com/in/{first_name.lower()}-{last_name.lower()}" if random.random() > 0.6 else None,
                random.random() > 0.8,
                random.choice(['active', 'active', 'inactive']),
                random.choice(LEAD_SOURCES),
                random.sample(['enterprise', 'decision-maker', 'technical', 'influencer'], random.randint(0, 2)) or None,
                fake.paragraph() if random.random() > 0.7 else None,
                created,
                created + timedelta(days=random.randint(0, 30)),
            ))
            
        columns = [
            'id', 'organization_id', 'first_name', 'last_name', 'email', 'phone',
            'mobile', 'job_title', 'department', 'linkedin_url', 'is_primary',
            'status', 'lead_source', 'tags', 'notes', 'created_at', 'updated_at'
        ]
        self.contact_ids = self._batch_insert('contacts', columns, data, returning='id')
        
    def _seed_leads(self):
        """Seed leads table."""
        count = self.profile['leads']
        print(f"Seeding {count} leads...")
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(180)
            status = random.choice(LEAD_STATUSES)
            data.append((
                str(uuid.uuid4()),
                fake.first_name(),
                fake.last_name(),
                fake.email(),
                fake.phone_number()[:50],
                fake.company(),
                fake.job(),
                random.choice(INDUSTRIES),
                random.choice(LEAD_SOURCES),
                status,
                random.choice(['hot', 'warm', 'cold']),
                random.randint(0, 100),
                random.choice(self.user_ids) if self.user_ids else None,
                created + timedelta(days=random.randint(1, 30)) if status == 'converted' else None,
                random.choice(self.contact_ids) if self.contact_ids and status == 'converted' else None,
                random.choice(self.org_ids) if self.org_ids and status == 'converted' else None,
                fake.paragraph() if random.random() > 0.5 else None,
                created,
                created + timedelta(days=random.randint(0, 14)),
            ))
            
        columns = [
            'id', 'first_name', 'last_name', 'email', 'phone', 'company_name',
            'job_title', 'industry', 'lead_source', 'status', 'rating', 'score',
            'assigned_to', 'converted_at', 'converted_contact_id', 'converted_organization_id',
            'notes', 'created_at', 'updated_at'
        ]
        self.lead_ids = self._batch_insert('leads', columns, data, returning='id')
        
    def _seed_opportunities(self):
        """Seed opportunities table."""
        count = self.profile['opportunities']
        print(f"Seeding {count} opportunities...")
        
        opp_types = ['New Business', 'Expansion', 'Renewal', 'Upsell', 'Cross-sell']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(365)
            stage_id = random.choice(self.stage_ids) if self.stage_ids else None
            status = 'won' if stage_id == self.stage_ids[4] else ('lost' if stage_id == self.stage_ids[5] else 'open')
            expected_close = created + timedelta(days=random.randint(30, 180))
            
            data.append((
                str(uuid.uuid4()),
                f"{fake.company()} - {random.choice(opp_types)}",
                random.choice(self.org_ids) if self.org_ids else None,
                random.choice(self.contact_ids) if self.contact_ids else None,
                stage_id,
                Decimal(str(random.randint(5000, 500000))),
                'USD',
                random.randint(10, 90),
                expected_close.date(),
                expected_close.date() if status in ('won', 'lost') else None,
                status,
                random.choice(opp_types),
                random.choice(LEAD_SOURCES),
                random.choice(self.user_ids) if self.user_ids else None,
                fake.paragraph(),
                fake.sentence() if random.random() > 0.5 else None,
                ', '.join(random.sample(['Competitor A', 'Competitor B', 'Competitor C', 'In-house'], random.randint(0, 2))) or None,
                created,
                self._updated_at(created, 30),
            ))
            
        columns = [
            'id', 'name', 'organization_id', 'contact_id', 'stage_id', 'amount',
            'currency', 'probability', 'expected_close_date', 'actual_close_date',
            'status', 'type', 'lead_source', 'assigned_to', 'description',
            'next_step', 'competitors', 'created_at', 'updated_at'
        ]
        self.opportunity_ids = self._batch_insert('opportunities', columns, data, returning='id')
        
    def _seed_deals(self):
        """Seed deals table."""
        count = self.profile['deals']
        print(f"Seeding {count} deals...")
        
        payment_terms = ['Net 30', 'Net 60', 'Net 90', 'Due on Receipt', '2/10 Net 30']
        billing_freq = ['monthly', 'quarterly', 'annually', 'one-time']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(365)
            # Start date is within 30 days after created (but created is already in the past)
            start = created + timedelta(days=random.randint(0, 30))
            # End date is 30-365 days after start
            end = start + timedelta(days=random.randint(30, 365))
            
            data.append((
                str(uuid.uuid4()),
                random.choice(self.opportunity_ids) if self.opportunity_ids and random.random() > 0.3 else None,
                random.choice(self.org_ids) if self.org_ids else None,
                random.choice(self.contact_ids) if self.contact_ids else None,
                f"Deal - {fake.company()}",
                Decimal(str(random.randint(10000, 1000000))),
                'USD',
                random.choice(['active', 'active', 'active', 'completed', 'cancelled']),
                start.date(),
                end.date(),
                f"CNT-{random.randint(10000, 99999)}",
                random.choice(payment_terms),
                random.choice(billing_freq),
                self._updated_at(created, 14),
                random.choice(self.user_ids) if self.user_ids else None,
                fake.paragraph() if random.random() > 0.5 else None,
                created,
                self._updated_at(created, 30),
            ))
            
        columns = [
            'id', 'opportunity_id', 'organization_id', 'contact_id', 'name',
            'amount', 'currency', 'status', 'start_date', 'end_date',
            'contract_number', 'payment_terms', 'billing_frequency', 'signed_at',
            'assigned_to', 'notes', 'created_at', 'updated_at'
        ]
        self.deal_ids = self._batch_insert('deals', columns, data, returning='id')
        
    def _seed_products(self):
        """Seed products table."""
        count = self.profile['products']
        print(f"Seeding {count} products...")
        
        product_names = [
            'Enterprise License', 'Professional License', 'Basic License',
            'Support Plan - Gold', 'Support Plan - Silver', 'Support Plan - Bronze',
            'Implementation Services', 'Training - Onsite', 'Training - Virtual',
            'Consulting - Strategy', 'Consulting - Technical', 'Data Migration',
            'API Access', 'Custom Integration', 'Hardware Module', 'Security Add-on',
        ]
        
        data = []
        for i in range(count):
            created = self._random_timestamp(365)
            name = product_names[i % len(product_names)] + (f" v{i // len(product_names) + 1}" if i >= len(product_names) else "")
            data.append((
                str(uuid.uuid4()),
                random.choice(self.category_ids) if self.category_ids else None,
                f"SKU-{i + 1:06d}",  # Sequential SKU to avoid duplicates
                name,
                fake.paragraph(),
                Decimal(str(random.randint(100, 50000))),
                'USD',
                random.choice(['license', 'seat', 'hour', 'unit', 'month']),
                True,
                random.random() > 0.2,
                Decimal(str(random.uniform(0, 0.15))),
                random.randint(0, 1000),
                random.randint(10, 100),
                created,
                created,
            ))
            
        columns = [
            'id', 'category_id', 'sku', 'name', 'description', 'unit_price',
            'currency', 'unit_of_measure', 'is_active', 'is_taxable', 'tax_rate',
            'stock_quantity', 'reorder_level', 'created_at', 'updated_at'
        ]
        self.product_ids = self._batch_insert('products', columns, data, returning='id')
        
    def _seed_price_book_entries(self):
        """Seed price_book_entries table."""
        if not self.price_book_id or not self.product_ids:
            return
            
        print("Seeding price book entries...")
        
        data = []
        for product_id in self.product_ids:
            # Standard pricing
            created = self._random_timestamp(365)
            data.append((
                str(uuid.uuid4()),
                self.price_book_id,
                product_id,
                Decimal(str(random.randint(100, 50000))),
                1,
                None,
                True,
                created,
                created,
            ))
            
            # Volume discount tiers
            if random.random() > 0.5:
                for qty, discount in [(10, 5), (50, 10), (100, 15)]:
                    data.append((
                        str(uuid.uuid4()),
                        self.price_book_id,
                        product_id,
                        Decimal(str(random.randint(100, 50000))),
                        qty,
                        Decimal(str(discount)),
                        True,
                        created,
                        created,
                    ))
                    
        columns = [
            'id', 'price_book_id', 'product_id', 'unit_price', 'min_quantity',
            'discount_percent', 'is_active', 'created_at', 'updated_at'
        ]
        self._batch_insert('price_book_entries', columns, data)
        
    def _seed_line_items(self):
        """Seed line_items table."""
        if not self.deal_ids or not self.product_ids:
            return
            
        print("Seeding line items...")
        
        data = []
        for deal_id in self.deal_ids:
            # 1-5 line items per deal
            for i in range(random.randint(1, 5)):
                quantity = Decimal(str(random.randint(1, 100)))
                unit_price = Decimal(str(random.randint(100, 10000)))
                discount = Decimal(str(random.uniform(0, 0.2)))
                subtotal = quantity * unit_price
                discount_amount = subtotal * discount
                tax_rate = Decimal('0.08')
                total = (subtotal - discount_amount) * (1 + tax_rate)
                
                created = self._random_timestamp(180)
                data.append((
                    str(uuid.uuid4()),
                    deal_id,
                    random.choice(self.product_ids),
                    fake.sentence(),
                    quantity,
                    unit_price,
                    discount * 100,
                    discount_amount,
                    tax_rate * 100,
                    subtotal,
                    total,
                    i,
                    created,
                    created,
                ))
                
        columns = [
            'id', 'deal_id', 'product_id', 'description', 'quantity', 'unit_price',
            'discount_percent', 'discount_amount', 'tax_rate', 'subtotal', 'total',
            'sort_order', 'created_at', 'updated_at'
        ]
        self._batch_insert('line_items', columns, data)
        
    def _seed_tickets(self):
        """Seed tickets table."""
        count = self.profile['tickets']
        print(f"Seeding {count} tickets...")
        
        subjects = [
            'Cannot login to system', 'Invoice discrepancy', 'Feature request',
            'Integration not working', 'Performance issue', 'Data export problem',
            'Permission error', 'Report not generating', 'API timeout',
            'Password reset needed', 'Account access issue', 'Billing question',
        ]
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(180)
            status = random.choice(TICKET_STATUSES)
            
            first_response = created + timedelta(hours=random.randint(1, 24)) if status != 'open' else None
            resolved = first_response + timedelta(hours=random.randint(1, 72)) if status in ('resolved', 'closed') else None
            closed = resolved + timedelta(hours=random.randint(1, 24)) if status == 'closed' else None
            
            data.append((
                str(uuid.uuid4()),
                random.choice(self.org_ids) if self.org_ids and random.random() > 0.2 else None,
                random.choice(self.contact_ids) if self.contact_ids and random.random() > 0.3 else None,
                random.choice(subjects) + f" - {fake.word()}",
                fake.paragraph(nb_sentences=3),
                status,
                random.choice(TICKET_PRIORITIES),
                random.choice(['bug', 'question', 'task', 'incident']),
                random.choice(TICKET_CATEGORIES),
                random.choice(['email', 'phone', 'web', 'chat']),
                random.choice(self.user_ids) if self.user_ids else None,
                first_response,
                resolved,
                closed,
                random.randint(1, 5) if status == 'closed' and random.random() > 0.3 else None,
                fake.sentence() if status == 'closed' and random.random() > 0.5 else None,
                random.sample(['urgent', 'vip', 'escalated', 'follow-up'], random.randint(0, 2)) or None,
                created,
                self._updated_at(created, 7),
            ))
            
        columns = [
            'id', 'organization_id', 'contact_id', 'subject', 'description',
            'status', 'priority', 'type', 'category', 'channel', 'assigned_to',
            'first_response_at', 'resolved_at', 'closed_at', 'satisfaction_rating',
            'satisfaction_comment', 'tags', 'created_at', 'updated_at'
        ]
        self.ticket_ids = self._batch_insert('tickets', columns, data, returning='id')
        
    def _seed_ticket_comments(self):
        """Seed ticket_comments table."""
        if not self.ticket_ids:
            return
            
        print("Seeding ticket comments...")
        
        data = []
        for ticket_id in self.ticket_ids:
            # 0-10 comments per ticket
            for _ in range(random.randint(0, 10)):
                created = self._random_timestamp(90)
                data.append((
                    str(uuid.uuid4()),
                    ticket_id,
                    random.choice(self.user_ids) if self.user_ids else None,
                    random.choice(['user', 'customer', 'system']),
                    fake.paragraph(),
                    random.random() > 0.7,
                    None,
                    created,
                    created,
                ))
                
        columns = [
            'id', 'ticket_id', 'author_id', 'author_type', 'content',
            'is_internal', 'attachments', 'created_at', 'updated_at'
        ]
        self._batch_insert('ticket_comments', columns, data)
        
    def _seed_activities(self):
        """Seed activities table."""
        count = self.profile['activities']
        print(f"Seeding {count} activities...")
        
        subjects = [
            'Follow up call', 'Product demo', 'Contract review', 'Weekly sync',
            'Quarterly review', 'Onboarding session', 'Training call', 'Support check-in',
            'Renewal discussion', 'Upsell opportunity', 'Issue resolution', 'Feedback session',
        ]
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(180)
            activity_type = random.choice(ACTIVITY_TYPES)
            status = random.choice(ACTIVITY_STATUSES)
            # Ensure start_time stays in the past (relative to created which is already in past)
            start = created - timedelta(days=random.randint(0, 30))
            duration = random.randint(15, 120)
            
            data.append((
                str(uuid.uuid4()),
                activity_type,
                random.choice(subjects),
                fake.paragraph() if random.random() > 0.5 else None,
                status,
                random.choice(['low', 'normal', 'high']),
                start,
                start + timedelta(minutes=duration),
                duration,
                fake.address() if activity_type == 'meeting' and random.random() > 0.5 else None,
                random.choice(['completed', 'rescheduled', 'no-show', 'interested']) if status == 'completed' else None,
                random.choice(self.org_ids) if self.org_ids and random.random() > 0.3 else None,
                random.choice(self.contact_ids) if self.contact_ids and random.random() > 0.4 else None,
                random.choice(self.opportunity_ids) if self.opportunity_ids and random.random() > 0.6 else None,
                random.choice(self.deal_ids) if self.deal_ids and random.random() > 0.7 else None,
                random.choice(self.user_ids) if self.user_ids else None,
                start if status == 'completed' else None,
                start - timedelta(hours=random.randint(1, 24)) if random.random() > 0.5 else None,
                False,
                False,
                None,
                created,
                self._updated_at(created, 7),
            ))
            
        columns = [
            'id', 'type', 'subject', 'description', 'status', 'priority',
            'start_time', 'end_time', 'duration_minutes', 'location', 'outcome',
            'organization_id', 'contact_id', 'opportunity_id', 'deal_id',
            'assigned_to', 'completed_at', 'reminder_at', 'is_all_day',
            'is_recurring', 'recurrence_rule', 'created_at', 'updated_at'
        ]
        self._batch_insert('activities', columns, data)
        
    def _seed_email_templates(self):
        """Seed email_templates table."""
        print("Seeding email templates...")
        
        templates = [
            ('Welcome Email', 'Welcome to {{company}}!', 'onboarding'),
            ('Follow Up', 'Following up on our conversation', 'sales'),
            ('Demo Invitation', 'Invitation to product demo', 'sales'),
            ('Quote Sent', 'Your quote is ready', 'sales'),
            ('Contract Ready', 'Contract ready for signature', 'sales'),
            ('Support Resolution', 'Your ticket has been resolved', 'support'),
            ('Renewal Reminder', 'Your subscription is up for renewal', 'billing'),
            ('Invoice', 'Invoice for {{month}}', 'billing'),
            ('Thank You', 'Thank you for your business', 'general'),
            ('Newsletter', 'Monthly newsletter', 'marketing'),
        ]
        
        data = []
        for name, subject, category in templates:
            created = self._random_timestamp(365)
            data.append((
                str(uuid.uuid4()),
                name,
                subject,
                fake.paragraph(nb_sentences=5),
                category,
                True,
                random.randint(0, 1000),
                random.choice(self.user_ids) if self.user_ids else None,
                created,
                created,
            ))
            
        columns = [
            'id', 'name', 'subject', 'body', 'category', 'is_active',
            'usage_count', 'created_by', 'created_at', 'updated_at'
        ]
        self.template_ids = self._batch_insert('email_templates', columns, data, returning='id')
        
    def _seed_email_logs(self):
        """Seed email_logs table."""
        count = self.profile['email_logs']
        print(f"Seeding {count} email logs...")
        
        statuses = ['sent', 'sent', 'sent', 'delivered', 'opened', 'clicked', 'bounced', 'failed']
        
        data = []
        for _ in range(count):
            created = self._random_timestamp(90)
            status = random.choice(statuses)
            sent = created
            opened = sent + timedelta(hours=random.randint(1, 48)) if status in ('opened', 'clicked') else None
            clicked = opened + timedelta(minutes=random.randint(1, 60)) if status == 'clicked' else None
            bounced = sent + timedelta(minutes=random.randint(1, 30)) if status == 'bounced' else None
            
            data.append((
                str(uuid.uuid4()),
                random.choice(self.template_ids) if self.template_ids and random.random() > 0.3 else None,
                random.choice(self.contact_ids) if self.contact_ids else None,
                random.choice(self.org_ids) if self.org_ids and random.random() > 0.5 else None,
                fake.company_email(),
                [fake.email()],
                [fake.email()] if random.random() > 0.8 else None,
                None,
                fake.sentence(),
                fake.paragraph(),
                status,
                sent,
                opened,
                clicked,
                bounced,
                'Mailbox not found' if status in ('bounced', 'failed') else None,
                None,
                created,
                created,
            ))
            
        columns = [
            'id', 'template_id', 'contact_id', 'organization_id', 'from_address',
            'to_addresses', 'cc_addresses', 'bcc_addresses', 'subject', 'body',
            'status', 'sent_at', 'opened_at', 'clicked_at', 'bounced_at',
            'error_message', 'metadata', 'created_at', 'updated_at'
        ]
        self._batch_insert('email_logs', columns, data)


def main():
    parser = argparse.ArgumentParser(description='Seed CRM database with test data')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5433, help='Database port')
    parser.add_argument('--database', default='crm_source', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='postgres', help='Database password')
    parser.add_argument('--profile', choices=PROFILES.keys(), default='small',
                       help='Data volume profile: small, medium, large, xlarge')
    parser.add_argument('--batch-size', type=int, default=1000, help='Insert batch size')
    
    args = parser.parse_args()
    
    conn_string = f"host={args.host} port={args.port} dbname={args.database} user={args.user} password={args.password}"
    
    print(f"Connecting to {args.database} on {args.host}:{args.port}")
    print(f"Profile: {args.profile} - {PROFILES[args.profile]}")
    
    seeder = DataSeeder(conn_string, args.profile, args.batch_size)
    seeder.run()


if __name__ == '__main__':
    main()
