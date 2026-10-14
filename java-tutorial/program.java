// Java Program
public class program {
    void main() {
        int[] numbers = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        for(int number : numbers) {
            if(number % 2 == 0) {
                System.out.println(number);
            }
        }
    }
}