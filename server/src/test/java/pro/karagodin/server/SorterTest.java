package pro.karagodin.server;

import org.junit.jupiter.api.Test;
import pro.karagodin.message.Data;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SorterTest {
	@Test
	public void test1() {
		Random random = new Random();
		List<Integer> list = new ArrayList<>(random.ints().limit(10000).boxed().toList());
		Data sendData = Data.newBuilder().addAllData(list).build();

		Data recievedData = Sorter.bubleSort(sendData);
		list.sort(Comparator.comparingInt(x -> x));
		assertEquals(list, recievedData.getDataList());
	}
}