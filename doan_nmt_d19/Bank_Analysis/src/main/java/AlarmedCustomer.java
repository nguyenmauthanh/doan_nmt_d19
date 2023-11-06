package src.main.java;

public class AlarmedCustomer {

    public final String id;
    public final String account;

    public AlarmedCustomer(){
        id = "";
        account = "";
    }

    public AlarmedCustomer(String data){
        String[] words = data.split(",");
        id = words[0];
        account = words[1];
    }

}
