from selenium import webdriver

# ChromeDriver path is automatically picked up from the PATH
driver = webdriver.Chrome()

# Open a webpage (e.g., ProFootballReference)
driver.get("https://www.pro-football-reference.com")

# Print the page title to verify it worked
print(driver.title)

# Close the WebDriver
driver.quit()