from com.join.quant.juejin.model import Mystrategy
if __name__ == '__main__':
    myStrategy = Mystrategy()
    myStrategy.on_login()
    myStrategy.on_error(122, 'dsds')
    print(myStrategy.position)