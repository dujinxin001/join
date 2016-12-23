from com.join.quant.juejin.model import Mystrategy
if __name__ == '__main__':
    myStrategy = Mystrategy(3333333)
    myStrategy.on_login()
    myStrategy.on_error(122, 'dsds')
    myStrategy.on_tick(1)
    print(myStrategy.position)
    print('111'+'.'+'dddd')
    
