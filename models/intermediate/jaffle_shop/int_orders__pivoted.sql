with payments as (
    select * from {{ source('stripe','payments')}}
),

pivoted as (
    select 
    orderid,
    {%- set payment_list = ['bank_transfer','gift_card','coupon','credit_card'] -%}
        {% for method in payment_list %}
            sum(case when paymentmethod = '{{method}}' then amount else 0 end) as {{method}}_amount 
            {%- if not loop.last-%} 
            ,
            {%- endif -%}
        {% endfor %}

   -- sum(case when paymentmethod = 'gift_card' then amount else 0 end) as gift_card_amount,
   --sum(case when paymentmethod = 'coupon' then amount else 0 end) as coupon_amount,
   -- sum(case when paymentmethod = 'credit_card' then amount else 0 end) as credit_card_amount

    from 
    payments

    where 
    status = 'success'

    group by 
    orderid
)

select * from pivoted order by orderid asc