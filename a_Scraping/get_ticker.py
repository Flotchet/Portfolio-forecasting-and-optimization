from selenium import webdriver
from selenium.webdriver.common.by import By

url = "https://marketstack.com/search"

#driver is firefox
driver = webdriver.Firefox()
driver.get(url)

for i in range(2048):
    #Get the table element
    table = driver.find_element(By.XPATH, "/html/body/div/section[2]/div/div[2]/table/tbody")
    #save the table into a csv
    with open(f"data/ticker{i}.csv", "w") as f:
        for row in table.find_elements(By.TAG_NAME, "tr"):
            for cell in row.find_elements(By.TAG_NAME, "td"):
                f.write(cell.text + ",")
            f.write("\n")

    #scroll from 1080px
    driver.execute_script("window.scrollTo(0, 1080)")

    #click the next button
    button = driver.find_element(By.XPATH, "/html/body/div/section[2]/div/div[2]/a[2]")
    button.click()


#close the driver
driver.close()
        

