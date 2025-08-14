import apache_beam as beam
import sys


city_zip_list = [
    ('Lexington', '40513'),
    ('Nashville', '37027'),
    ('Lexington', '40502'),
    ('Seattle', '98125'),
    ('Mountain View', '94041'),
    ('Seattle', '98133'),
    ('Lexington', '40591'),
    ('Mountain View', '94085'),
]

# New data structure: salesperson id and sales amount
salesperson_sales_list = [
    ('SP001', 1200.50),
    ('SP002', 950.00),
    ('SP001', 300.75),
    ('SP003', 2100.00),
    ('SP002', 400.25),
    ('SP004', 1800.00),
    ('SP003', 500.00),
    ('SP001', 700.00),
]

# List 1: order numbers and amounts
order_numbers_amounts = [
    ('ORD1001', 250.00),
    ('ORD1002', 120.50),
    ('ORD1003', 75.25),
    ('ORD1004', 600.00),
    ('ORD1005', 320.10),
    ('ORD1006', 150.75),
    ('ORD1007', 980.00),
    ('ORD1008', 45.00),
]

# List 2: order numbers and delivery dates
order_numbers_delivery_dates = [
    ('ORD1001', '2025-08-14'),
    ('ORD1002', '2025-08-15'),
    ('ORD1003', '2025-08-16'),
    ('ORD1004', '2025-08-17'),
    ('ORD1005', '2025-08-18'),
    ('ORD1006', '2025-08-19'),
    ('ORD1007', '2025-08-20'),
    ('ORD1008', '2025-08-21'),
]

p = beam.Pipeline(argv=sys.argv)

citycodes = p | 'CreateCityCodes' >> beam.Create(city_zip_list)
# citycodes | "print1" >> beam.Map(print)

grouped = citycodes | beam.GroupByKey()
grouped | "print2" >> beam.Map(print)

sales = p | 'CreateSales' >> beam.Create(salesperson_sales_list)
sales_total = sales | beam.CombineGlobally(sum)
sales_total | "print3" >> beam.Map(print)

sales_total_by_rep = sales | beam.CombinePerKey(sum)
sales_total_by_rep | "print4" >> beam.Map(print)

orders_amounts = p | 'CreateOrderNumbers' >> beam.Create(order_numbers_amounts)
orders_amounts | "print5" >> beam.Map(print)

orders_delivery_dates = p | 'CreateOrderDeliveryDates' >> beam.Create(order_numbers_delivery_dates)
orders_delivery_dates | "print6" >> beam.Map(print)

joined = {'orders':orders_amounts, 'shipping': orders_delivery_dates} | beam.CoGroupByKey()
joined | "print7" >> beam.Map(print)

p.run().wait_until_finish()