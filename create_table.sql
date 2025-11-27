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
    pro_id int references Product(), -- serial number
    cus_id char(13) references Customer(driver_licence)
    quantity float default 1,
    ref_date date,
    order_num int primary key
)
