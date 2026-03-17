from sql_blocks import Parser, Schema


def create_public_schema():
    if Parser.public_schema:
        return
    Parser.public_schema = Schema('''
        create table Customer(
            id int primary key,
            driver_licence char(13), 
            name varchar(255),
            region int /*     1=North
                            2=South
                            3=East
                            4=West
            ****************************************************/
        );
        create table Product(
            serial_number int primary key,
            name varchar(255) unique,
            price float not null
        );
        create table Sales(
            pro_id int references Product, -- serial number
            cus_id char(13) references Customer(id)
            quantity float default 1,
            ref_date date,
            order_num int primary key
        )
    ''')

def remove_public_schema():
    Parser.public_schema = None
