package pro.karagodin.server;

import pro.karagodin.message.Data;

import java.util.ArrayList;
import java.util.List;

public class Sorter {
	public static Data bubleSort(Data data) {
		List<Integer> list = new ArrayList<>(data.getDataList());
		Integer temp;
		for (int i = 0; i < list.size(); i++) {
			for (int j = 1; j < list.size() - i; j++) {
				if (list.get(j - 1) > list.get(j)){
					temp = list.get(j - 1);
					list.set(j - 1, list.get(j));
					list.set(j, temp);
				}
			}
		}
		return Data.newBuilder().addAllData(list).build();
	}
}
