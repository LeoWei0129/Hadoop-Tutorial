package mapreduce.order.topn;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class ObjectSerializeToFileTest {
    public static void main(String[] args) throws IOException {
        OrderBean orderBean = new OrderBean();

        // case 1
        //ArrayList arrayList = new ArrayList();
        // case 2
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("D:\\apache_ecosystem" +
                "\\mapreduce_test\\ordertopn\\object_serialize_test.txt", true));

        orderBean.setParams("a", "phone", "water", 4.5f, 3);
        //arrayList.add(orderBean);
        oos.writeObject(orderBean);

        orderBean.setParams("b", "pen", "juice", 3.2f, 3);
        //arrayList.add(orderBean);
        oos.writeObject(orderBean);

        orderBean.setParams("c", "keyboard", "cola", 2.5f, 1);
        //arrayList.add(orderBean);
        oos.writeObject(orderBean);

        // µ²ªG:
        // [OrderBean [orderId=c, userId=keyboard, pdtName=cola, price=2.5, number=1, totalPrice=2.5],
        // OrderBean [orderId=c, userId=keyboard, pdtName=cola, price=2.5, number=1, totalPrice=2.5],
        // OrderBean [orderId=c, userId=keyboard, pdtName=cola, price=2.5, number=1, totalPrice=2.5]]
        //System.out.println(arrayList);
        oos.close();
    }
}
