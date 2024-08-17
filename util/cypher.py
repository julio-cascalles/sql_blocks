from enum import Enum
import re
from sql_blocks.sql_blocks import *


class TokenType(Enum):
    TABLE = 1
    FIELD = 2
    WHERE = 3
    LJOIN = 4
    F_KEY = 5

class Cypher:
    def __init__(self, cmd: str):
        SQLObject.ALIAS_FUNC = lambda t: t[0].lower()
        self.queries = []
        tt: TokenType = TokenType.TABLE
        for token in re.split('([()?,]|->)', cmd):
            token = token.strip()
            if not token:
                continue
            match token:
                case '(':
                    if tt == TokenType.LJOIN:
                        tt = TokenType.F_KEY
                    else:
                        tt = TokenType.FIELD
                case '?':
                    tt = TokenType.WHERE
                case ',':
                    if tt in [TokenType.WHERE, TokenType.FIELD, TokenType.F_KEY]:
                        tt = TokenType.FIELD
                    else:
                        raise SyntaxError('Invalid comma in expression.')
                case ')':
                    tt = TokenType.TABLE
                    table = ''
                case '->':
                    tt = TokenType.LJOIN
                case _:
                    match tt:
                        case TokenType.TABLE | TokenType.LJOIN:
                            self.queries.append( Select(token) )
                        case TokenType.FIELD:
                            Field.add(token, self.queries[-1])
                        case TokenType.WHERE:
                            field, condition = [
                                t for t in re.split(r'(^\w+)', token.strip()) if t
                            ]
                            Where(condition).add(field, self.queries[-1])
                        case TokenType.F_KEY:
                            curr, last = [self.queries[i] for i in (-1, -2)]
                            pk_field = last.values[SELECT][-1].split('.')[-1]
                            last.delete(pk_field, [SELECT])
                            k = ForeignKey.get_key(last, curr)
                            ForeignKey.references[k] = (pk_field, token)


if __name__ == "__main__":
    CYPHER_QUERY = "Pais(nome?PIB > 1000,sigla)->Bandeira(pais, cor)->Cor(hexa?nome = 'Vermelho')"
    print('='*100)
    print('  Extraindo e combinando 3 queries do comando CYPHER:  '.center(100, '#'))
    print('\t', CYPHER_QUERY)
    print('#'*100)
    p, b, c = Cypher(CYPHER_QUERY).queries
    print('='*100)
    print(p)
    print('-'*100)
    print(b)
    print('-'*100)
    print(c)
    print('-'*100)
    print(p + (b + c))
    print('='*100)
