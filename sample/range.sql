SELECT
    p.name,    
    p.age /*  generation:
        gen_X=50
        gen_Z=17
        alpha=10
        bommer=70
        gen_Y=21
    */
FROM
    Person p
ORDER BY
    p.name